using Orleans.Concurrency;
using Orleans.Runtime;
using System;
using System.Threading.Tasks;

namespace Orleans.Indexing
{
    [Reentrant]
    internal class ReincarnatedIndexWorkflowQueueHandler : Grain, IIndexWorkflowQueueHandler
    {
        private IIndexWorkflowQueueHandler _base;

        internal IndexManager IndexManager => IndexManager.GetIndexManager(ref __indexManager, base.ServiceProvider);
        private IndexManager __indexManager;

        public override Task OnActivateAsync()
        {
            DelayDeactivation(ReincarnatedIndexWorkflowQueue.ACTIVE_FOR_A_DAY);
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
                    throw new WorkflowIndexException("The primary key for IndexWorkflowQueueSystemTarget should only contain a single special character '-', while it contains multiple." +
                                                     " The primary key is '" + oldParentSystemTargetRef.GetPrimaryKeyString() + "'");
                }

                Type grainInterfaceType = this.IndexManager.CachedTypeResolver.ResolveType(parts[0]);
                int queueSequenceNumber = int.Parse(parts[1]);

                GrainReference thisRef = this.AsWeaklyTypedReference();
                _base = new IndexWorkflowQueueHandlerBase(this.IndexManager, grainInterfaceType, queueSequenceNumber,
                                                          oldParentSystemTargetRef.SystemTargetSilo,
                                                          true /*otherwise it shouldn't have reached here!*/, thisRef);
            }
            return Task.CompletedTask;
        }

        public Task HandleWorkflowsUntilPunctuation(Immutable<IndexWorkflowRecordNode> workflowRecordsHead)
        {
            return _base.HandleWorkflowsUntilPunctuation(workflowRecordsHead);
        }
    }
}
