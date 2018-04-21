using System;

namespace Orleans.Indexing.Tests
{
    [Serializable]
    public class Player3PropertiesNonFaultTolerantLazy : IPlayerProperties
    {
        public int Score { get; set; }

        [ActiveIndex(ActiveIndexType.HashIndexPartitionedByKeyHash/*, IsEager = false*/)]
        public string Location { get; set; }
    }

    public interface IPlayer3GrainNonFaultTolerantLazy : IPlayerGrain, IIndexableGrain<Player3PropertiesNonFaultTolerantLazy>
    {
    }
}
