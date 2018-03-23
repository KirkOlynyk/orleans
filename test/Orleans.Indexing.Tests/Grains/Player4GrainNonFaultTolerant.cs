using System;
using Orleans.Providers;

namespace Orleans.Indexing.Tests
{
    [Serializable]
    public class Player4GrainStateNonFaultTolerant : Player4PropertiesNonFaultTolerant, IPlayerState
    {
        public string Email { get; set; }
    }

    /// <summary>
    /// A simple grain that represent a player in a game
    /// </summary>
    [StorageProvider(ProviderName = "MemoryStore")]
    public class Player4GrainNonFaultTolerant : PlayerGrainNonFaultTolerant<Player4GrainStateNonFaultTolerant, Player4PropertiesNonFaultTolerant>, IPlayer4GrainNonFaultTolerant
    {
    }
}
