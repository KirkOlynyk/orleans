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

        internal IndexWorkflowQueueHandlerSystemTarget(IndexingManager indexingManager, Type iGrainType, int queueSeqNum, bool isDefinedAsFaultTolerantGrain)
            : base(IndexWorkflowQueueHandlerBase.CreateIndexWorkflowQueueHandlerGrainId(iGrainType, queueSeqNum), indexingManager.SiloAddress, indexingManager.LoggerFactory)
        {
            _base = new IndexWorkflowQueueHandlerBase(indexingManager, iGrainType, queueSeqNum, indexingManager.SiloAddress,
                                                      isDefinedAsFaultTolerantGrain, this.AsWeaklyTypedReference());
        }

        public Task HandleWorkflowsUntilPunctuation(Immutable<IndexWorkflowRecordNode> workflowRecordsHead)
        {
            return _base.HandleWorkflowsUntilPunctuation(workflowRecordsHead);
        }

        public Task Initialize(IIndexWorkflowQueue oldParentSystemTarget)
        {
            throw new NotSupportedException();
        }
    }
}
