using System;

namespace Orleans.Indexing
{
    public class IndexingOptions
    {
        public IndexingOptions()
        {
            this.MaxHashBuckets = -1;
        }

        public int MaxHashBuckets { get; set; }

        public void ConfigureWorkflow()
        {
            // Placeholder
        }

        public void ConfigureTransactions()
        {
            // Placeholder
        }
    }
}
