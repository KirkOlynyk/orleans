namespace Orleans.Indexing
{
    internal static class IndexingConstants
    {
        public const string INDEXING_STORAGE_PROVIDER_NAME = "IndexingStorageProvider";
        public const string INDEXING_WORKFLOWQUEUE_STORAGE_PROVIDER_NAME = "IndexingWorkflowQueueStorageProvider";
        public const string INDEXING_STREAM_PROVIDER_NAME = "IndexingStreamProvider";

        public const int INDEX_WORKFLOW_QUEUE_HANDLER_SYSTEM_TARGET_TYPE_CODE = 251;
        public const int INDEX_WORKFLOW_QUEUE_SYSTEM_TARGET_TYPE_CODE = 252;
        public const int HASH_INDEX_PARTITIONED_PER_SILO_BUCKET_SYSTEM_TARGET_TYPE_CODE = 253;
    }
}
