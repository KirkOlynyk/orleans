using System;
using Orleans.Indexing;

namespace Orleans.Indexing.Tests
{
    [Serializable]
    public class Player5PropertiesNonFaultTolerant : IPlayerProperties
    {
        [Index(typeof(IActiveHashIndexSingleBucket<string, IPlayer5GrainNonFaultTolerant>), IsEager = true, IsUnique = true)]
        public int Score { get; set; }

        [Index(typeof(ActiveHashIndexPartitionedPerKey<string, IPlayer5GrainNonFaultTolerant>), IsEager = true, IsUnique = true)]
        public string Location { get; set; }
    }

    public interface IPlayer5GrainNonFaultTolerant : IPlayerGrain, IIndexableGrain<Player5PropertiesNonFaultTolerant>
    {
    }
}
