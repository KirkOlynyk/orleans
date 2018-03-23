using System;
using Orleans.Providers;

namespace Orleans.Indexing.Tests
{
    [Serializable]
    public class PlayerChain1GrainState : PlayerChain1Properties, IPlayerState
    {
        public string Email { get; set; }
    }

    /// <summary>
    /// A simple grain that represent a player in a game
    /// </summary>
    public class PlayerChain1Grain : PlayerGrainNonFaultTolerant<PlayerChain1GrainState, PlayerChain1Properties>, IPlayerChain1Grain
    {
    }
}
