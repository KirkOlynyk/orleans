using System;
using Orleans.Providers;

namespace Orleans.Indexing.Tests
{
    [Serializable]
    public class Player1GrainStateNonFaultTolerantLazy : Player1PropertiesNonFaultTolerantLazy, IPlayerState
    {
        public string Email { get; set; }
    }

    /// <summary>
    /// A simple grain that represent a player in a game
    /// </summary>
    [StorageProvider(ProviderName = "MemoryStore")]
    public class Player1GrainNonFaultTolerantLazy : PlayerGrainNonFaultTolerant<Player1GrainStateNonFaultTolerantLazy, Player1PropertiesNonFaultTolerantLazy>, IPlayer1GrainNonFaultTolerantLazy
    {
    }
}
