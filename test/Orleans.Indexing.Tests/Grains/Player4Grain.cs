using System;
using Orleans.Providers;

namespace Orleans.Indexing.Tests
{
    [Serializable]
    public class Player4GrainState : Player4Properties, IPlayerState
    {
        public string Email { get; set; }
    }

    /// <summary>
    /// A simple grain that represent a player in a game
    /// </summary>
    [StorageProvider(ProviderName = "GrainStore")]
    public class Player4Grain : PlayerGrain<Player4GrainState, Player4Properties>, IPlayer4Grain
    {
    }
}
