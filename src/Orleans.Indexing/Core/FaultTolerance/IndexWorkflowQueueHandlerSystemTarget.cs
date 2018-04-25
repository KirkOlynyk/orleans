using Orleans.Concurrency;
using Orleans.Runtime;
using System;
using System.Threading.Tasks;

namespace Orleans.Indexing
{
    [Reentrant]
    internal class IndexWorkflowQueueHandlerSystemTarget : SystemTarget, IIndexWorkflowQueueHandler
    {
        private IIndexWorkflowQueueHandler _base;

        internal IndexWorkflowQueueHandlerSystemTarget(SiloIndexManager siloIndexManager, Type iGrainType, int queueSeqNum, bool isDefinedAsFaultTolerantGrain)
            : base(IndexWorkflowQueueHandlerBase.CreateIndexWorkflowQueueHandlerGrainId(iGrainType, queueSeqNum), siloIndexManager.SiloAddress, siloIndexManager.LoggerFactory)
        {
            GrainReference thisRef = this.AsGrainReference(siloIndexManager.GrainReferenceRuntime, siloIndexManager.SiloAddress, out GrainId _);
            _base = new IndexWorkflowQueueHandlerBase(siloIndexManager, iGrainType, queueSeqNum, siloIndexManager.SiloAddress, isDefinedAsFaultTolerantGrain, thisRef);
        }

        public Task HandleWorkflowsUntilPunctuation(Immutable<IndexWorkflowRecordNode> workflowRecordsHead)
            => _base.HandleWorkflowsUntilPunctuation(workflowRecordsHead);

        public Task Initialize(IIndexWorkflowQueue oldParentSystemTarget)
            => throw new NotSupportedException();
    }
}
