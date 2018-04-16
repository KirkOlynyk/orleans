using Orleans.Concurrency;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.Indexing
{
    internal class IndexWorkflowQueueHandlerBase : IIndexWorkflowQueueHandler
    {
        private IIndexWorkflowQueue __workflowQueue;
        private IIndexWorkflowQueue WorkflowQueue => __workflowQueue ?? InitIndexWorkflowQueue();

        private int _queueSeqNum;
        private Type _iGrainType;

        private bool _isDefinedAsFaultTolerantGrain;
        private bool _hasAnyTotalIndex;
        private bool HasAnyTotalIndex { get { EnsureIndexes(); return _hasAnyTotalIndex; } }
        private bool IsFaultTolerant => _isDefinedAsFaultTolerantGrain && HasAnyTotalIndex;

        private IDictionary<string, Tuple<object, object, object>> __indexes;

        private IDictionary<string, Tuple<object, object, object>> Indexes => EnsureIndexes();

        private SiloAddress _silo;
        private IndexManager _indexManager;
        private GrainReference _parent;

        internal IndexWorkflowQueueHandlerBase(IndexManager indexManager, Type iGrainType, int queueSeqNum, SiloAddress silo, bool isDefinedAsFaultTolerantGrain, GrainReference parent)
        {
            _iGrainType = iGrainType;
            _queueSeqNum = queueSeqNum;
            _isDefinedAsFaultTolerantGrain = isDefinedAsFaultTolerantGrain;
            _hasAnyTotalIndex = false;
            __indexes = null;
            __workflowQueue = null;
            _silo = silo;
            _indexManager = indexManager;
            _parent = parent;
        }

        public async Task HandleWorkflowsUntilPunctuation(Immutable<IndexWorkflowRecordNode> workflowRecords)
        {
            try
            {
                var workflows = workflowRecords.Value;
                while (workflows != null)
                {
                    var grainsToActiveWorkflows = IsFaultTolerant ? await GetActiveWorkflowsListsFromGrains(workflows) : null;
                    var updatesToIndexes = CreateAMapForUpdatesToIndexes();
                    PopulateUpdatesToIndexes(workflows, updatesToIndexes, grainsToActiveWorkflows);
                    await Task.WhenAll(PrepareIndexUpdateTasks(updatesToIndexes));
                    if (IsFaultTolerant)
                    {
                        Task.WhenAll(RemoveFromActiveWorkflowsInGrainsTasks(grainsToActiveWorkflows)).Ignore();
                    }
                    workflows = (await WorkflowQueue.GiveMoreWorkflowsOrSetAsIdle()).Value;
                }
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        private IList<Task> RemoveFromActiveWorkflowsInGrainsTasks(Dictionary<IIndexableGrain, HashSet<Guid>> grainsToActiveWorkflows)
            => grainsToActiveWorkflows.Select(pair => pair.Key.RemoveFromActiveWorkflowIds(pair.Value)).ToList();

        private IList<Task<bool>> PrepareIndexUpdateTasks(Dictionary<string, IDictionary<IIndexableGrain, IList<IMemberUpdate>>> updatesToIndexes)
        {
            IList<Task<bool>> updateIndexTasks = new List<Task<bool>>();
            foreach (var indexEntry in Indexes)
            {
                var idxInfo = indexEntry.Value;
                var updatesToIndex = updatesToIndexes[indexEntry.Key];
                if (updatesToIndex.Count() > 0)
                {
                    updateIndexTasks.Add(((IIndexInterface)idxInfo.Item1).ApplyIndexUpdateBatch(
                                                _indexManager.RuntimeClient, updatesToIndex.AsImmutable(),
                                                ((IndexMetaData)idxInfo.Item2).IsUniqueIndex(), (IndexMetaData)idxInfo.Item2, _silo));
                }
            }

            return updateIndexTasks;
        }

        private void PopulateUpdatesToIndexes(IndexWorkflowRecordNode currentWorkflow, Dictionary<string, IDictionary<IIndexableGrain,
                                              IList<IMemberUpdate>>> updatesToIndexes, Dictionary<IIndexableGrain, HashSet<Guid>> grainsToActiveWorkflows)
        {
            bool faultTolerant = IsFaultTolerant;
            while (!currentWorkflow.IsPunctuation())
            {
                IndexWorkflowRecord workflowRec = currentWorkflow.WorkflowRecord;
                IIndexableGrain g = workflowRec.Grain;
                bool existsInActiveWorkflows = false;
                if (faultTolerant)
                {
                    if (grainsToActiveWorkflows.TryGetValue(g, out HashSet<Guid> activeWorkflowRecs))
                    {
                        if (activeWorkflowRecs.Contains(workflowRec.WorkflowId))
                        {
                            existsInActiveWorkflows = true;
                        }
                    }
                }

                foreach (var updates in currentWorkflow.WorkflowRecord.MemberUpdates)
                {
                    IMemberUpdate updt = updates.Value;
                    if (updt.GetOperationType() != IndexOperationType.None)
                    {
                        string index = updates.Key;
                        var updatesToIndex = updatesToIndexes[index];
                        if (!updatesToIndex.TryGetValue(g, out IList<IMemberUpdate> updatesList))
                        {
                            updatesList = new List<IMemberUpdate>();
                            updatesToIndex.Add(g, updatesList);
                        }

                        if (!faultTolerant || existsInActiveWorkflows)
                        {
                            updatesList.Add(updt);
                        }
                        //if the workflow record does not exist in the list of active work-flows
                        //and the index is fault-tolerant, we should make sure that tentative updates
                        //to unique indexes are undone
                        else if (((IndexMetaData)Indexes[index].Item2).IsUniqueIndex())
                        {
                            //reverse a possible remaining tentative record from the index
                            updatesList.Add(new MemberUpdateReverseTentative(updt));
                        }
                    }
                }
                currentWorkflow = currentWorkflow.Next;
            }
        }

        private static HashSet<Guid> emptyHashset = new HashSet<Guid>();

        private async Task<Dictionary<IIndexableGrain, HashSet<Guid>>> GetActiveWorkflowsListsFromGrains(IndexWorkflowRecordNode currentWorkflow)
        {
            var result = new Dictionary<IIndexableGrain, HashSet<Guid>>();
            var grains = new List<IIndexableGrain>();
            var activeWorkflowsSetsTasks = new List<Task<Immutable<HashSet<Guid>>>>();

            while (!currentWorkflow.IsPunctuation())
            {
                IIndexableGrain g = currentWorkflow.WorkflowRecord.Grain;
                foreach (var updates in currentWorkflow.WorkflowRecord.MemberUpdates)
                {
                    IMemberUpdate updt = updates.Value;
                    if (updt.GetOperationType() != IndexOperationType.None && !result.ContainsKey(g))
                    {
                        result.Add(g, emptyHashset);
                        grains.Add(g);
                        activeWorkflowsSetsTasks.Add(g.AsReference<IIndexableGrain>(_indexManager.RuntimeClient.InternalGrainFactory, _iGrainType).GetActiveWorkflowIdsList());
                    }
                }
                currentWorkflow = currentWorkflow.Next;
            }

            if (activeWorkflowsSetsTasks.Count() > 0)
            {
                Immutable<HashSet<Guid>>[] activeWorkflowsSets = await Task.WhenAll(activeWorkflowsSetsTasks);
                for (int i = 0; i < activeWorkflowsSets.Length; ++i)
                {
                    result[grains[i]] = activeWorkflowsSets[i].Value;
                }
            }

            return result;
        }

        private Dictionary<string, IDictionary<IIndexableGrain, IList<IMemberUpdate>>> CreateAMapForUpdatesToIndexes()
        {
            var updatesToIndexes = new Dictionary<string, IDictionary<IIndexableGrain, IList<IMemberUpdate>>>();
            foreach (string index in Indexes.Keys)
            {
                updatesToIndexes.Add(index, new Dictionary<IIndexableGrain, IList<IMemberUpdate>>());
            }

            return updatesToIndexes;
        }

        private IDictionary<string, Tuple<object, object, object>> EnsureIndexes()
        {
            if (__indexes == null)
            {
                __indexes = _indexManager.IndexFactory.GetIndexes(_iGrainType);
                _hasAnyTotalIndex = __indexes.Values.Any(idxInfo => idxInfo.Item1 is ITotalIndex);
            }
            return __indexes;
        }

        private IIndexWorkflowQueue InitIndexWorkflowQueue()
            => __workflowQueue = _parent.IsSystemTarget
                    ? _indexManager.RuntimeClient.InternalGrainFactory.GetSystemTarget<IIndexWorkflowQueue>(IndexWorkflowQueueBase.CreateIndexWorkflowQueueGrainId(_iGrainType, _queueSeqNum), _silo)
                    : _indexManager.GrainFactory.GetGrain<IIndexWorkflowQueue>(IndexWorkflowQueueBase.CreateIndexWorkflowQueuePrimaryKey(_iGrainType, _queueSeqNum));

        public static GrainId CreateIndexWorkflowQueueHandlerGrainId(Type grainInterfaceType, int queueSeqNum)
            => IndexExtensions.GetSystemTargetGrainId(IndexingConstants.INDEX_WORKFLOW_QUEUE_HANDLER_SYSTEM_TARGET_TYPE_CODE,
                                                      IndexWorkflowQueueBase.CreateIndexWorkflowQueuePrimaryKey(grainInterfaceType, queueSeqNum));

        public Task Initialize(IIndexWorkflowQueue oldParentSystemTarget)
            => throw new NotSupportedException();
    }
}
