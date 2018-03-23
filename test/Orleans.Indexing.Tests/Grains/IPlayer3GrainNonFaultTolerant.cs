using System;
using Orleans.Indexing;

namespace Orleans.Indexing.Tests
{
    [Serializable]
    public class Player3PropertiesNonFaultTolerant : IPlayerProperties
    {
        public int Score { get; set; }

        [ActiveIndex(ActiveIndexType.HashIndexPartitionedByKeyHash, IsEager = true)]
        public string Location { get; set; }
    }

    public interface IPlayer3GrainNonFaultTolerant : IPlayerGrain, IIndexableGrain<Player3PropertiesNonFaultTolerant>
    {
    }
}
