using System;
using Orleans.Providers;

namespace Orleans.Indexing.Tests
{
    [Serializable]
    public class Player3GrainState : Player3Properties, IPlayerState
    {
        public string Email { get; set; }
    }

    /// <summary>
    /// A simple grain that represent a player in a game
    /// </summary>
    [StorageProvider(ProviderName = "GrainStore")]
    public class Player3Grain : PlayerGrain<Player3GrainState, Player3Properties>, IPlayer3Grain
    {
    }
}
