using System;
using Orleans.Indexing;

namespace Orleans.Indexing.Tests
{
    [Serializable]
    public class Player5PropertiesNonFaultTolerantLazy : IPlayerProperties
    {
        [Index(typeof(IActiveHashIndexSingleBucket<string, IPlayer5GrainNonFaultTolerantLazy>)/*, IsEager = false*/, IsUnique = true)]
        public int Score { get; set; }

        [Index(typeof(ActiveHashIndexPartitionedPerKey<string, IPlayer5GrainNonFaultTolerantLazy>)/*, IsEager = false*/, IsUnique = true)]
        public string Location { get; set; }
    }

    public interface IPlayer5GrainNonFaultTolerantLazy : IPlayerGrain, IIndexableGrain<Player5PropertiesNonFaultTolerantLazy>
    {
    }
}
