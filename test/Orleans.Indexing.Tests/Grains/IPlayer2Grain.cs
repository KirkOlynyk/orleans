using System;
using Orleans.Indexing;

namespace Orleans.Indexing.Tests
{
    [Serializable]
    public class Player2Properties : IPlayerProperties
    {
        public int Score { get; set; }

        [ActiveIndex(ActiveIndexType.HashIndexPartitionedBySilo)]
        public string Location { get; set; }
    }

    public interface IPlayer2Grain : IPlayerGrain, IIndexableGrain<Player2Properties>
    {
    }
}
