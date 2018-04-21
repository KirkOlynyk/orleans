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
        private bool HasAnyTotalIndex { get { EnsureGrainIndexes(); return _hasAnyTotalIndex; } }
        private bool IsFaultTolerant => _isDefinedAsFaultTolerantGrain && HasAnyTotalIndex;

        private NamedIndexMap __grainIndexes;

        private NamedIndexMap GrainIndexes => EnsureGrainIndexes();

        private SiloAddress _silo;
        private IndexManager _indexManager;
        private GrainReference _parent;

        internal IndexWorkflowQueueHandlerBase(IndexManager indexManager, Type iGrainType, int queueSeqNum, SiloAddress silo, bool isDefinedAsFaultTolerantGrain, GrainReference parent)
        {
            _iGrainType = iGrainType;
            _queueSeqNum = queueSeqNum;
            _isDefinedAsFaultTolerantGrain = isDefinedAsFaultTolerantGrain;
            _hasAnyTotalIndex = false;
            __grainIndexes = null;
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
            foreach (var indexEntry in GrainIndexes)
            {
                var idxInfo = indexEntry.Value;
                var updatesToIndex = updatesToIndexes[indexEntry.Key];
                if (updatesToIndex.Count() > 0)
                {
                    updateIndexTasks.Add(idxInfo.IndexInterface.ApplyIndexUpdateBatch(_indexManager.RuntimeClient, updatesToIndex.AsImmutable(),
                                                                                      idxInfo.MetaData.IsUniqueIndex, idxInfo.MetaData, _silo));
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
                bool existsInActiveWorkflows = faultTolerant && grainsToActiveWorkflows.TryGetValue(g, out HashSet<Guid> activeWorkflowRecs)
                                                             && activeWorkflowRecs.Contains(workflowRec.WorkflowId);

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
                        // If the workflow record does not exist in the list of active work-flows and the index is fault-tolerant,
                        // we should make sure that tentative updates to unique indexes are undone.
                        else if (GrainIndexes[index].MetaData.IsUniqueIndex)
                        {
                            // Reverse a possible remaining tentative record from the index
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
            => GrainIndexes.Keys.Select(key => new { key, dict = new Dictionary<IIndexableGrain, IList<IMemberUpdate>>() as IDictionary<IIndexableGrain, IList<IMemberUpdate>> })
                                .ToDictionary(it => it.key, it => it.dict);

        private NamedIndexMap EnsureGrainIndexes()
        {
            if (__grainIndexes == null)
            {
                __grainIndexes = _indexManager.IndexFactory.GetGrainIndexes(_iGrainType);
                _hasAnyTotalIndex = __grainIndexes.HasAnyTotalIndex;
            }
            return __grainIndexes;
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
