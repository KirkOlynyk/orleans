using System;
using Orleans.Providers;

namespace Orleans.Indexing.Tests
{
    [Serializable]
    public class Player4GrainStateNonFaultTolerantLazy : Player4PropertiesNonFaultTolerantLazy, IPlayerState
    {
        public string Email { get; set; }
    }

    /// <summary>
    /// A simple grain that represent a player in a game
    /// </summary>
    [StorageProvider(ProviderName = "MemoryStore")]
    public class Player4GrainNonFaultTolerantLazy : PlayerGrainNonFaultTolerant<Player4GrainStateNonFaultTolerantLazy, Player4PropertiesNonFaultTolerantLazy>, IPlayer4GrainNonFaultTolerantLazy
    {
    }
}
