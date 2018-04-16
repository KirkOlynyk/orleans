using Orleans.Concurrency;

namespace Orleans.Indexing
{
    /// <summary>
    /// This is a marker interface for <see cref="IActiveHashIndexPartitionedPerSilo{K, V}"/> generic interface
    /// </summary>
    public interface IActiveHashIndexPartitionedPerSilo : IGrain
    {
    }

    /// <summary>
    /// The interface for the <see cref="ActiveHashIndexPartitionedPerSiloBucketImpl"/> system target,
    /// which is created in order to guide Orleans to find the grain instances more efficiently.
    /// 
    /// Generic SystemTargets are not supported yet, and that's why the interface is non-generic.
    /// </summary>
    /// <typeparam name="K">type of hash-index key</typeparam>
    /// <typeparam name="V">type of grain that is being indexed</typeparam>
    //internal interface ActiveHashIndexPartitionedPerSiloBucket<K, V> : ISystemTarget, HashIndexInterface<K, V> where V : IIndexableGrain
    [Unordered]
    public interface IActiveHashIndexPartitionedPerSilo<K, V> : IActiveHashIndexPartitionedPerSilo, IHashIndexInterface<K, V> where V : IIndexableGrain
    {
    }
}
