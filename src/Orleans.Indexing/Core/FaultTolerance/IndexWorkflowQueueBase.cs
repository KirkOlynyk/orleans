using Microsoft.Extensions.DependencyInjection;
using Orleans.Concurrency;
using Orleans.Runtime;
using Orleans.Storage;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Indexing
{
    /// <summary>
    /// To minimize the number of RPCs, we process index updates for each grain
    /// on the silo where the grain is active. To do this processing, each silo
    /// has one or more IndexWorkflowQueue system-targets for each grain class,
    /// up to the number of hardware threads. A system-target is a grain that
    /// belongs to a specific silo.
    /// + Each of these system-targets has a queue of workflowRecords, which describe
    ///   updates that must be propagated to indexes.Each workflowRecord contains
    ///   the following information:
    ///    - workflowID: grainID + a sequence number
    ///    - memberUpdates: the updated values of indexed fields
    ///  
    ///   Ordinarily, these workflowRecords are for grains that are active on
    ///   IndexWorkflowQueue's silo. (This may not be true for short periods when
    ///   a grain migrates to another silo or after the silo recovers from failure).
    /// 
    /// + The IndexWorkflowQueue grain Q has a dictionary updatesOnWait is an
    ///   in-memory dictionary that maps each grain G to the workflowRecords for G
    ///   that are waiting for be updated
    /// </summary>
    internal class IndexWorkflowQueueBase : IIndexWorkflowQueue
    {
        //the persistent state of IndexWorkflowQueue, including:
        // - doubly linked list of workflowRecordds
        // - the identity of the IndexWorkflowQueue system target
        protected IndexWorkflowQueueState queueState;

        //the tail of workflowRecords doubly linked list
        internal IndexWorkflowRecordNode _workflowRecordsTail;

        //the storage provider for index work-flow queue
        private IStorageProvider _storageProvider;
        private IStorageProvider StorageProvider { get { return _storageProvider ?? InitStorageProvider(); } }

        private int _queueSeqNum;
        private Type _iGrainType;

        //0: uninitialized, 1: has some Total Indexes, -1: does not have any Total Index
        private sbyte __hasAnyTotalIndex;
        private bool HasAnyTotalIndex { get { return __hasAnyTotalIndex == 0 ? InitHasAnyTotalIndex() : __hasAnyTotalIndex > 0; } }

        private bool _isDefinedAsFaultTolerantGrain;
        private bool IsFaultTolerant { get { return _isDefinedAsFaultTolerantGrain && HasAnyTotalIndex; } }

        private IIndexWorkflowQueueHandler __handler;
        private IIndexWorkflowQueueHandler Handler { get { return __handler ?? InitWorkflowQueueHandler(); } }

        private int _isHandlerWorkerIdle;

        /// <summary>
        /// This lock is used to queue all the writes to the storage
        /// and do them in a single batch, i.e., group commit
        /// 
        /// Works hand-in-hand with pendingWriteRequests and writeRequestIdGen.
        /// </summary>
        private AsyncLock _writeLock;

        /// <summary>
        /// Creates a unique ID for each write request to the storage.
        /// 
        /// The values generated by this ID generator are used in pendingWriteRequests
        /// </summary>
        private int _writeRequestIdGen;

        /// <summary>
        /// All the write requests that are waiting behind write_lock are accumulated
        /// in this data structure, and all of them will be done at once.
        /// </summary>
        private HashSet<int> _pendingWriteRequests;

        public const int BATCH_SIZE = int.MaxValue;

        public static int NUM_AVAILABLE_INDEX_WORKFLOW_QUEUES { get { return Environment.ProcessorCount; } }

        private SiloAddress _silo;
        private IndexManager _indexManager;

        private GrainReference _parent;

        internal IndexWorkflowQueueBase(IndexManager indexManager, Type grainInterfaceType, int queueSequenceNumber, SiloAddress silo,
                                        bool isDefinedAsFaultTolerantGrain, GrainId grainId, GrainReference parent)
        {
            queueState = new IndexWorkflowQueueState(grainId, silo);
            _iGrainType = grainInterfaceType;
            _queueSeqNum = queueSequenceNumber;

            _workflowRecordsTail = null;
            _storageProvider = null;
            __handler = null;
            _isHandlerWorkerIdle = 1;

            _isDefinedAsFaultTolerantGrain = isDefinedAsFaultTolerantGrain;
            __hasAnyTotalIndex = 0;

            _writeLock = new AsyncLock();
            _writeRequestIdGen = 0;
            _pendingWriteRequests = new HashSet<int>();

            _silo = silo;
            _indexManager = indexManager;
            _parent = parent;
        }

        private IIndexWorkflowQueueHandler InitWorkflowQueueHandler()
        {
            __handler = _parent.IsSystemTarget
                ? _indexManager.RuntimeClient.InternalGrainFactory.GetSystemTarget<IIndexWorkflowQueueHandler>(
                        IndexWorkflowQueueHandlerBase.CreateIndexWorkflowQueueHandlerGrainId(_iGrainType, _queueSeqNum), _silo)
                : _indexManager.GrainFactory.GetGrain<IIndexWorkflowQueueHandler>(CreateIndexWorkflowQueuePrimaryKey(_iGrainType, _queueSeqNum));
            return __handler;
        }

        public Task AddAllToQueue(Immutable<List<IndexWorkflowRecord>> workflowRecords)
        {
            List<IndexWorkflowRecord> newWorkflows = workflowRecords.Value;

            foreach (IndexWorkflowRecord newWorkflow in newWorkflows)
            {
                AddToQueueNonPersistent(newWorkflow);
            }

            InitiateWorkerThread();
            return IsFaultTolerant ? PersistState() : Task.CompletedTask;
        }

        public Task AddToQueue(Immutable<IndexWorkflowRecord> workflow)
        {
            IndexWorkflowRecord newWorkflow = workflow.Value;

            AddToQueueNonPersistent(newWorkflow);

            InitiateWorkerThread();
            return IsFaultTolerant ? PersistState() : Task.CompletedTask;
        }

        private void AddToQueueNonPersistent(IndexWorkflowRecord newWorkflow)
        {
            IndexWorkflowRecordNode newWorkflowNode = new IndexWorkflowRecordNode(newWorkflow);
            if (_workflowRecordsTail == null) //if the list is empty
            {
                _workflowRecordsTail = newWorkflowNode;
                queueState.State.WorkflowRecordsHead = newWorkflowNode;
            }
            else // otherwise append to the end of the list
            {
                _workflowRecordsTail.Append(newWorkflowNode, ref _workflowRecordsTail);
            }
        }

        public Task RemoveAllFromQueue(Immutable<List<IndexWorkflowRecord>> workflowRecords)
        {
            List<IndexWorkflowRecord> newWorkflows = workflowRecords.Value;

            foreach (IndexWorkflowRecord newWorkflow in newWorkflows)
            {
                RemoveFromQueueNonPersistent(newWorkflow);
            }

            return IsFaultTolerant ? PersistState() : Task.CompletedTask;
        }

        private void RemoveFromQueueNonPersistent(IndexWorkflowRecord newWorkflow)
        {
            IndexWorkflowRecordNode current = queueState.State.WorkflowRecordsHead;
            while (current != null)
            {
                if (newWorkflow.Equals(current.WorkflowRecord))
                {
                    current.Remove(ref queueState.State.WorkflowRecordsHead, ref _workflowRecordsTail);
                    return;
                }
                current = current.Next;
            }
        }

        private void InitiateWorkerThread()
        {
            if (Interlocked.Exchange(ref _isHandlerWorkerIdle, 0) == 1)
            {
                IndexWorkflowRecordNode punctuatedHead = AddPuctuationAt(BATCH_SIZE);
                Handler.HandleWorkflowsUntilPunctuation(punctuatedHead.AsImmutable()).Ignore();
            }
        }

        private IndexWorkflowRecordNode AddPuctuationAt(int batchSize)
        {
            if (_workflowRecordsTail == null) throw new WorkflowIndexException("Adding a punctuation to an empty work-flow queue is not possible.");

            var punctutationHead = queueState.State.WorkflowRecordsHead;
            if (punctutationHead.IsPunctuation()) throw new WorkflowIndexException("The element at the head of work-flow queue cannot be a punctuation.");

            if (batchSize == int.MaxValue)
            {
                var punctuation = _workflowRecordsTail.AppendPunctuation(ref _workflowRecordsTail);
                return punctutationHead;
            }
            var punctutationLoc = punctutationHead;

            int i = 1;
            while (i < batchSize && punctutationLoc.Next != null)
            {
                punctutationLoc = punctutationLoc.Next;
                ++i;
            }
            punctutationLoc.AppendPunctuation(ref _workflowRecordsTail);
            return punctutationHead;
        }

        private List<IndexWorkflowRecord> RemoveFromQueueUntilPunctuation(IndexWorkflowRecordNode from)
        {
            List<IndexWorkflowRecord> workflowRecords = new List<IndexWorkflowRecord>();
            if (from != null && !from.IsPunctuation())
            {
                workflowRecords.Add(from.WorkflowRecord);
            }

            IndexWorkflowRecordNode tmp = from.Next;
            while (tmp != null && !tmp.IsPunctuation())
            {
                workflowRecords.Add(tmp.WorkflowRecord);
                tmp = tmp.Next;
                tmp.Prev.Clean();
            }

            if (tmp == null) from.Remove(ref queueState.State.WorkflowRecordsHead, ref _workflowRecordsTail);
            else
            {
                from.Next = tmp;
                tmp.Prev = from;
                from.Remove(ref queueState.State.WorkflowRecordsHead, ref _workflowRecordsTail);
                tmp.Remove(ref queueState.State.WorkflowRecordsHead, ref _workflowRecordsTail);
            }

            return workflowRecords;
        }

        private async Task PersistState()
        {
            //create a write-request ID, which is used for group commit
            int writeRequestId = ++_writeRequestIdGen;

            //add the write-request ID to the pending write requests
            _pendingWriteRequests.Add(writeRequestId);

            //wait before any previous write is done
            using (await _writeLock.LockAsync())
            {
                //if the write request was not already handled
                //by a previous group write attempt
                if (_pendingWriteRequests.Contains(writeRequestId))
                {
                    //clear all pending write requests, as this attempt will do them all.
                    _pendingWriteRequests.Clear();
                    //write the state back to the storage
#if false //vv2err In v1 IExtendedStorageProvider/.WriteStateWithoutEtagCheckAsync was added to Corleans in support of indexing
                    IExtendedStorageProvider extendedSP = StorageProvider as IExtendedStorageProvider;
                    await (extendedSP == null
                        ? StorageProvider.WriteStateAsync("Orleans.Indexing.IndexWorkflowQueue-" + TypeUtils.GetFullName(_iGrainType), _parent, State)
                        : extendedSP.WriteStateWithoutEtagCheckAsync("Orleans.Indexing.IndexWorkflowQueue-" + TypeUtils.GetFullName(_iGrainType), _parent, State));
#else
                    await StorageProvider.WriteStateAsync("Orleans.Indexing.IndexWorkflowQueue-" + TypeUtils.GetFullName(_iGrainType), _parent, queueState);
#endif
                }
                //else
                //{
                //    Nothing! It's already been done by a previous worker.
                //}
            }
        }

        public Task<Immutable<IndexWorkflowRecordNode>> GiveMoreWorkflowsOrSetAsIdle()
        {
            List<IndexWorkflowRecord> removedWorkflows = RemoveFromQueueUntilPunctuation(queueState.State.WorkflowRecordsHead);
            if (IsFaultTolerant)
            {
                //The task of removing the work-flow record IDs from the grain
                //runs in parallel with persisting the state. At this point, there
                //is a possibility that some work-flow record IDs do not get removed
                //from the indexable grains while the work-flow record is removed
                //from the queue. This is fine, because having some dangling work-flow
                //IDs in some indexable grains is harmless.
                //TODO: add a garbage collector that runs once in a while and removes
                //      the dangling work-flow IDs (i.e., the work-flow IDs that exist in the
                //      indexable grain, but its corresponding work-flow record does not exist
                //      in the work-flow queue.
                //Task.WhenAll(
                //    RemoveWorkflowRecordsFromIndexableGrains(removedWorkflows),
                PersistState(//)
            ).Ignore();
            }

            if (_workflowRecordsTail == null)
            {
                _isHandlerWorkerIdle = 1;
                return Task.FromResult(new Immutable<IndexWorkflowRecordNode>(null));
            }
            else
            {
                _isHandlerWorkerIdle = 0;
                return Task.FromResult(AddPuctuationAt(BATCH_SIZE).AsImmutable());
            }
        }

        private bool InitHasAnyTotalIndex() // vv2 convert to bool? and LINQ
        {
            var indexes = _indexManager.IndexFactory.GetIndexes(_iGrainType);
            foreach (var idxInfo in indexes.Values)
            {
                if (idxInfo.Item1 is ITotalIndex)
                {
                    __hasAnyTotalIndex = 1;
                    return true;
                }
            }
            __hasAnyTotalIndex = -1;
            return false;
        }

        private IStorageProvider InitStorageProvider()
        {
            //vv2err return _storageProvider = _runtimeClient.Catalog.SetupStorageProvider(typeof(IndexWorkflowQueueSystemTarget));    // vv2 Catalog.SetupStorageProvider not implementable
            return null;
        }

        public Task<Immutable<List<IndexWorkflowRecord>>> GetRemainingWorkflowsIn(HashSet<Guid> activeWorkflowsSet)
        {
            var result = new List<IndexWorkflowRecord>();
            IndexWorkflowRecordNode current = queueState.State.WorkflowRecordsHead;
            while (current != null)
            {
                if (activeWorkflowsSet.Contains(current.WorkflowRecord.WorkflowId))
                {
                    result.Add(current.WorkflowRecord);
                }
                current = current.Next;
            }
            return Task.FromResult(result.AsImmutable());
        }

        public Task Initialize(IIndexWorkflowQueue oldParentSystemTarget)
        {
            throw new NotSupportedException();
        }

#region STATIC HELPER FUNCTIONS
        public static GrainId CreateIndexWorkflowQueueGrainId(Type grainInterfaceType, int queueSeqNum)
        {
            return IndexExtensions.GetSystemTargetGrainId(IndexingConstants.INDEX_WORKFLOW_QUEUE_SYSTEM_TARGET_TYPE_CODE,
                                          CreateIndexWorkflowQueuePrimaryKey(grainInterfaceType, queueSeqNum));
        }

        public static string CreateIndexWorkflowQueuePrimaryKey(Type grainInterfaceType, int queueSeqNum)
        {
            return TypeUtils.GetFullName(grainInterfaceType) + "-" + queueSeqNum;
        }

        public static GrainId GetIndexWorkflowQueueGrainIdFromGrainHashCode(Type grainInterfaceType, int grainHashCode)
        {
            int queueSeqNum = StorageProviderUtils.PositiveHash(grainHashCode, NUM_AVAILABLE_INDEX_WORKFLOW_QUEUES);
            return IndexExtensions.GetSystemTargetGrainId(IndexingConstants.INDEX_WORKFLOW_QUEUE_SYSTEM_TARGET_TYPE_CODE,
                                          CreateIndexWorkflowQueuePrimaryKey(grainInterfaceType, queueSeqNum));
        }

        public static IIndexWorkflowQueue GetIndexWorkflowQueueFromGrainHashCode(IndexManager indexManager, Type grainInterfaceType, int grainHashCode, SiloAddress siloAddress)
        {
            return indexManager.GetSystemTarget<IIndexWorkflowQueue>(
                GetIndexWorkflowQueueGrainIdFromGrainHashCode(grainInterfaceType, grainHashCode),
                siloAddress
            );
        }
#endregion STATIC HELPER FUNCTIONS
    }
}
