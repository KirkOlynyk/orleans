using System;
using Orleans.Providers;

namespace Orleans.Indexing.Tests
{
    [Serializable]
    public class Player5GrainStateNonFaultTolerant : Player5PropertiesNonFaultTolerant, IPlayerState
    {
        public string Email { get; set; }
    }

    /// <summary>
    /// A simple grain that represent a player in a game
    /// </summary>
    [StorageProvider(ProviderName = "MemoryStore")]
    public class Player5GrainNonFaultTolerant : PlayerGrainNonFaultTolerant<Player5GrainStateNonFaultTolerant, Player5PropertiesNonFaultTolerant>, IPlayer5GrainNonFaultTolerant
    {
    }
}
