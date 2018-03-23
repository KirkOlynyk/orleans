using System;

namespace Orleans.Indexing
{
    public class IndexingOptions
    {
        public IndexingOptions()
        {
            UseDefaults();
        }

        /// <summary>
        /// The number of new Transaction Ids allocated on every write to the log.
        /// To avoid writing to log on every transaction start, transaction Ids are allocated in batches.
        /// </summary>
        public bool UseTransactions { get; set; }

        private void UseDefaults()
        {
            this.UseTransactions = false;
        }
    }
}
