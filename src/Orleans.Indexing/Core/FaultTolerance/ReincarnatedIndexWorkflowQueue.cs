using Orleans.Concurrency;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.Indexing
{
    [Reentrant]
    internal class ReincarnatedIndexWorkflowQueue : Grain, IIndexWorkflowQueue
    {
        internal static TimeSpan ACTIVE_FOR_A_DAY = TimeSpan.FromDays(1);
        private IndexWorkflowQueueBase _base;

        internal IndexingManager IndexingManager => IndexingManager.GetIndexingManager(ref __indexingManager, base.ServiceProvider);
        private IndexingManager __indexingManager;

        public override Task OnActivateAsync()
        {
            DelayDeactivation(ACTIVE_FOR_A_DAY);
            return base.OnActivateAsync();
        }

        public Task Initialize(IIndexWorkflowQueue oldParentSystemTarget)
        {
            if (_base == null)
            {
                GrainReference oldParentSystemTargetRef = oldParentSystemTarget.AsWeaklyTypedReference();
                string[] parts = oldParentSystemTargetRef.GetPrimaryKeyString().Split('-');
                if (parts.Length != 2)
                {
                    throw new Exception("The primary key for IndexWorkflowQueueSystemTarget should only contain a single special character '-', while it contains multiple. The primary key is '" + oldParentSystemTargetRef.GetPrimaryKeyString() + "'");
                }

                Type grainInterfaceType = this.IndexingManager.CachedTypeResolver.ResolveType(parts[0]);
                int queueSequenceNumber = int.Parse(parts[1]);

                GrainReference thisRef = this.AsWeaklyTypedReference();
                _base = new IndexWorkflowQueueBase(this.IndexingManager, grainInterfaceType, queueSequenceNumber,
                                                   oldParentSystemTargetRef.SystemTargetSilo, true, thisRef.GrainId, thisRef);
            }
            return Task.CompletedTask;
        }

        public Task AddAllToQueue(Immutable<List<IndexWorkflowRecord>> workflowRecords)
        {
            return _base.AddAllToQueue(workflowRecords);
        }

        public Task AddToQueue(Immutable<IndexWorkflowRecord> workflowRecord)
        {
            return _base.AddToQueue(workflowRecord);
        }

        public Task<Immutable<List<IndexWorkflowRecord>>> GetRemainingWorkflowsIn(HashSet<Guid> activeWorkflowsSet)
        {
            return _base.GetRemainingWorkflowsIn(activeWorkflowsSet);
        }

        public Task<Immutable<IndexWorkflowRecordNode>> GiveMoreWorkflowsOrSetAsIdle()
        {
            return _base.GiveMoreWorkflowsOrSetAsIdle();
        }

        public Task RemoveAllFromQueue(Immutable<List<IndexWorkflowRecord>> workflowRecords)
        {
            return _base.RemoveAllFromQueue(workflowRecords);
        }
    }
}
