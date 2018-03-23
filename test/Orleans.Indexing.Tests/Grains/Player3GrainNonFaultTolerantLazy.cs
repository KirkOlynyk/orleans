using System;
using Orleans.Providers;

namespace Orleans.Indexing.Tests
{
    [Serializable]
    public class Player3GrainStateNonFaultTolerantLazy : Player3PropertiesNonFaultTolerantLazy, IPlayerState
    {
        public string Email { get; set; }
    }

    /// <summary>
    /// A simple grain that represent a player in a game
    /// </summary>
    [StorageProvider(ProviderName = "MemoryStore")]
    public class Player3GrainNonFaultTolerantLazy : PlayerGrainNonFaultTolerant<Player3GrainStateNonFaultTolerantLazy, Player3PropertiesNonFaultTolerantLazy>, IPlayer3GrainNonFaultTolerantLazy
    {
    }
}
