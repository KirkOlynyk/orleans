using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Indexing.Tests
{
    public abstract class ChainedBucketIndexingSingleSiloRunner : IndexingTestRunnerBase
    {
        protected ChainedBucketIndexingSingleSiloRunner(BaseIndexingFixture fixture, ITestOutputHelper output)
            : base(fixture, output)
        {
        }

        /// <summary>
        /// Tests basic functionality of HashIndexSingleBucket with chained buckets
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexLookup1()
        {
            IPlayerChain1Grain p1 = base.GetGrain<IPlayerChain1Grain>(1);
            await p1.SetLocation("Seattle");

            IPlayerChain1Grain p2 = base.GetGrain<IPlayerChain1Grain>(2);
            IPlayerChain1Grain p3 = base.GetGrain<IPlayerChain1Grain>(3);
            IPlayerChain1Grain p4 = base.GetGrain<IPlayerChain1Grain>(4);
            IPlayerChain1Grain p5 = base.GetGrain<IPlayerChain1Grain>(5);
            IPlayerChain1Grain p6 = base.GetGrain<IPlayerChain1Grain>(6);
            IPlayerChain1Grain p7 = base.GetGrain<IPlayerChain1Grain>(7);
            IPlayerChain1Grain p8 = base.GetGrain<IPlayerChain1Grain>(8);
            IPlayerChain1Grain p9 = base.GetGrain<IPlayerChain1Grain>(9);
            IPlayerChain1Grain p10 = base.GetGrain<IPlayerChain1Grain>(10);

            await p2.SetLocation("San Jose");
            await p3.SetLocation("San Fransisco");
            await p4.SetLocation("Bellevue");
            await p5.SetLocation("Redmond");
            await p6.SetLocation("Kirkland");
            await p7.SetLocation("Kirkland");
            await p8.SetLocation("Kirkland");
            await p9.SetLocation("Seattle");
            await p10.SetLocation("Kirkland");

            var locIdx = await base.GetAndWaitForIndex<string, IPlayerChain1Grain>("__Location");

            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayerChain1Grain, PlayerChain1Properties>("Seattle"));
            Assert.Equal(4, await this.CountPlayersStreamingIn<IPlayerChain1Grain, PlayerChain1Properties>("Kirkland"));

            await p8.Deactivate();
            await p9.Deactivate();
            Thread.Sleep(1000);

            Assert.Equal(1, await this.CountPlayersStreamingIn<IPlayerChain1Grain, PlayerChain1Properties>("Seattle"));
            Assert.Equal(3, await this.CountPlayersStreamingIn<IPlayerChain1Grain, PlayerChain1Properties>("Kirkland"));

            p10 = base.GetGrain<IPlayerChain1Grain>(10);
            Assert.Equal("Kirkland", await p10.GetLocation());
        }

        /// <summary>
        /// Tests basic functionality of ActiveHashIndexPartitionedPerSiloImpl with 1 Silo
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexLookup2()
        {
            IPlayer2GrainNonFaultTolerant p1 = base.GetGrain<IPlayer2GrainNonFaultTolerant>(1);
            await p1.SetLocation("Tehran");

            IPlayer2GrainNonFaultTolerant p2 = base.GetGrain<IPlayer2GrainNonFaultTolerant>(2);
            IPlayer2GrainNonFaultTolerant p3 = base.GetGrain<IPlayer2GrainNonFaultTolerant>(3);

            await p2.SetLocation("Tehran");
            await p3.SetLocation("Yazd");

            var locIdx = await base.GetAndWaitForIndex<string, IPlayer2GrainNonFaultTolerant>("__Location");

            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayer2GrainNonFaultTolerant, Player2PropertiesNonFaultTolerant>("Tehran"));

            await p2.Deactivate();
            Thread.Sleep(1000);

            Assert.Equal(1, await this.CountPlayersStreamingIn<IPlayer2GrainNonFaultTolerant, Player2PropertiesNonFaultTolerant>("Tehran"));

            p2 = base.GetGrain<IPlayer2GrainNonFaultTolerant>(2);
            Assert.Equal("Tehran", await p2.GetLocation());

            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayer2GrainNonFaultTolerant, Player2PropertiesNonFaultTolerant>("Tehran"));
        }

        /// <summary>
        /// Tests basic functionality of HashIndexPartitionedPerKey
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexLookup4()
        {
            IPlayer3GrainNonFaultTolerant p1 = base.GetGrain<IPlayer3GrainNonFaultTolerant>(1);
            await p1.SetLocation("Seattle");

            IPlayer3GrainNonFaultTolerant p2 = base.GetGrain<IPlayer3GrainNonFaultTolerant>(2);
            IPlayer3GrainNonFaultTolerant p3 = base.GetGrain<IPlayer3GrainNonFaultTolerant>(3);

            await p2.SetLocation("Seattle");
            await p3.SetLocation("San Fransisco");

            var locIdx = await base.GetAndWaitForIndex<string, IPlayer3GrainNonFaultTolerant>("__Location");

            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayer3GrainNonFaultTolerant, Player3PropertiesNonFaultTolerant>("Seattle"));

            await p2.Deactivate();
            Thread.Sleep(1000);

            Assert.Equal(1, await this.CountPlayersStreamingIn<IPlayer3GrainNonFaultTolerant, Player3PropertiesNonFaultTolerant>("Seattle"));

            p2 = base.GetGrain<IPlayer3GrainNonFaultTolerant>(2);
            Assert.Equal("Seattle", await p2.GetLocation());

            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayer3GrainNonFaultTolerant, Player3PropertiesNonFaultTolerant>("Seattle"));
        }
    }
}
