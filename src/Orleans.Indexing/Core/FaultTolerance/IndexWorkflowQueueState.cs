using Orleans.Runtime;
using System;

namespace Orleans.Indexing
{
    /// <summary>
    /// The persistent unit for storing the information for an <see cref="IndexWorkflowQueueGrainService"/>
    /// </summary>
    [Serializable]
    internal class IndexWorkflowQueueState : GrainState<IndexWorkflowQueueEntry>
    {
        public IndexWorkflowQueueState(SiloAddress silo) : base(new IndexWorkflowQueueEntry(silo))
        {
        }
    }

    /// <summary>
    /// All the information stored for a single <see cref="IndexWorkflowQueueGrainService"/>
    /// </summary>
    [Serializable]
    internal class IndexWorkflowQueueEntry
    {
        // Updates that must be propagated to indexes.
        internal IndexWorkflowRecordNode WorkflowRecordsHead;

        internal SiloAddress Silo { get; }

        public IndexWorkflowQueueEntry(SiloAddress silo)
        {
            this.WorkflowRecordsHead = null;
            this.Silo = silo;
        }
    }
}
