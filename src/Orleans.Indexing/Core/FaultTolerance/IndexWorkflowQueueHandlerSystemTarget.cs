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
            : base(IndexWorkflowQueueHandlerBase.CreateIndexWorkflowQueueHandlerGrainReference(siloIndexManager, iGrainType, queueSeqNum, siloIndexManager.SiloAddress),
                                                                                               siloIndexManager.LoggerFactory)
        {
            _base = new IndexWorkflowQueueHandlerBase(siloIndexManager, iGrainType, queueSeqNum, siloIndexManager.SiloAddress, isDefinedAsFaultTolerantGrain,
                                                      () => base.GetGrainReference());  // lazy is needed because the runtime isn't attached until Registered
        }

        public Task HandleWorkflowsUntilPunctuation(Immutable<IndexWorkflowRecordNode> workflowRecordsHead)
            => _base.HandleWorkflowsUntilPunctuation(workflowRecordsHead);

        public Task Initialize(IIndexWorkflowQueue oldParentSystemTarget)
            => throw new NotSupportedException();
    }
}
