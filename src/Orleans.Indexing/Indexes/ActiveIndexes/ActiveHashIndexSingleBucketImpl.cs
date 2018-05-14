using Orleans.Concurrency;
using Orleans.Runtime;
using Orleans.Providers;

namespace Orleans.Indexing
{
    /// <summary>
    /// A simple implementation of a single-grain in-memory hash-index
    /// </summary>
    /// <typeparam name="K">type of hash-index key</typeparam>
    /// <typeparam name="V">type of grain that is being indexed</typeparam>
    [StorageProvider(ProviderName = IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
    [Reentrant]
    public class ActiveHashIndexSingleBucketImpl<K, V> : HashIndexSingleBucket<K, V>, IActiveHashIndexSingleBucket<K, V> where V : class, IIndexableGrain
    {
        internal override IIndexInterface<K, V> GetNextBucket()
        {
            var NextBucket = this.GrainFactory.GetGrain<IActiveHashIndexSingleBucket<K, V>>(IndexUtils.GetNextIndexBucketIdInChain(this.AsWeaklyTypedReference()));
            this.State.NextBucket = NextBucket.AsWeaklyTypedReference();
            return NextBucket;
        }
    }
}
