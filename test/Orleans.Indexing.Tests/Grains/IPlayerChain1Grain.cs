using System;
using Orleans.Indexing;

namespace Orleans.Indexing.Tests
{
    [Serializable]
    public class PlayerChain1Properties : IPlayerProperties
    {
        [Index]
        public int Score { get; set; }
        
        [ActiveIndex(ActiveIndexType.HashIndexSingleBucket,true,5)]
        public string Location { get; set; }
    }

    public interface IPlayerChain1Grain : IPlayerGrain, IIndexableGrain<PlayerChain1Properties>
    {
    }
}
