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

        internal IndexWorkflowQueueHandlerSystemTarget(IndexManager indexManager, Type iGrainType, int queueSeqNum, bool isDefinedAsFaultTolerantGrain)
            : base(IndexWorkflowQueueHandlerBase.CreateIndexWorkflowQueueHandlerGrainId(iGrainType, queueSeqNum), indexManager.SiloAddress, indexManager.LoggerFactory)
        {
            _base = new IndexWorkflowQueueHandlerBase(indexManager, iGrainType, queueSeqNum, indexManager.SiloAddress,
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
