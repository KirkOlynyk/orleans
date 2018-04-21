using Orleans.Concurrency;
using Orleans.Runtime;
using Orleans.Providers;

namespace Orleans.Indexing
{
    /// <summary>
    /// A simple implementation of a single-bucket in-memory hash-index
    /// </summary>
    /// <typeparam name="K">type of hash-index key</typeparam>
    /// <typeparam name="V">type of grain that is being indexed</typeparam>
    [StorageProvider(ProviderName = Constants.MEMORY_STORAGE_PROVIDER_NAME)]
    [Reentrant]
    public class ActiveHashIndexPartitionedPerKeyBucketImpl<K, V> : HashIndexPartitionedPerKeyBucket<K, V>, IActiveHashIndexPartitionedPerKeyBucket<K, V>
        where V : class, IIndexableGrain
    {
        internal override IIndexInterface<K, V> GetNextBucket()
        {
            var NextBucket = GrainFactory.GetGrain<ActiveHashIndexPartitionedPerKeyBucketImpl<K, V>>(IndexUtils.GetNextIndexBucketIdInChain(this.AsWeaklyTypedReference()));
            State.NextBucket = NextBucket.AsWeaklyTypedReference();
            return NextBucket;
        }
    }
}
