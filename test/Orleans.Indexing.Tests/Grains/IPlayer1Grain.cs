using System;
using Orleans.Indexing;

namespace Orleans.Indexing.Tests
{
    [Serializable]
    public class Player1Properties : IPlayerProperties
    {
        public int Score { get; set; }

        [TotalIndex]
        public string Location { get; set; }
    }

    public interface IPlayer1Grain : IPlayerGrain, IIndexableGrain<Player1Properties>
    {
    }
}
