using System;
using Orleans.Providers;

namespace Orleans.Indexing.Tests
{
    [Serializable]
    public class Player2GrainStateNonFaultTolerantLazy : Player2PropertiesNonFaultTolerantLazy, IPlayerState
    {
        public string Email { get; set; }
    }

    /// <summary>
    /// A simple grain that represent a player in a game
    /// </summary>
    [StorageProvider(ProviderName = "MemoryStore")]
    public class Player2GrainNonFaultTolerantLazy : PlayerGrainNonFaultTolerant<Player2GrainStateNonFaultTolerantLazy, Player2PropertiesNonFaultTolerantLazy>, IPlayer2GrainNonFaultTolerantLazy
    {
    }
}
