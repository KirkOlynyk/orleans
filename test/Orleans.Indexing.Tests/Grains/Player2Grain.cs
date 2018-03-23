using System;
using Orleans.Providers;

namespace Orleans.Indexing.Tests
{
    [Serializable]
    public class Player2GrainState : Player2Properties, IPlayerState
    {
        public string Email { get; set; }
    }

    /// <summary>
    /// A simple grain that represent a player in a game
    /// </summary>
    [StorageProvider(ProviderName = "GrainStore")]
    public class Player2Grain : PlayerGrain<Player2GrainState, Player2Properties>, IPlayer2Grain
    {
    }
}
