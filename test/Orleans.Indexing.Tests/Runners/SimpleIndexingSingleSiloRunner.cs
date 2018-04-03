using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using System.Threading;

namespace Orleans.Indexing.Tests
{
    public abstract class SimpleIndexingSingleSiloRunner : IndexingTestRunnerBase
    {
        protected SimpleIndexingSingleSiloRunner(BaseIndexingFixture fixture, ITestOutputHelper output)
            : base(fixture, output)
        {
        }

        /// <summary>
        /// Tests basic functionality of HashIndexSingleBucket
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexLookup1()
        {
            var p1 = base.GetGrain<IPlayer1GrainNonFaultTolerant>(1);
            await p1.SetLocation("Seattle");

            var p2 = base.GetGrain<IPlayer1GrainNonFaultTolerant>(2);
            var p3 = base.GetGrain<IPlayer1GrainNonFaultTolerant>(3);

            await p2.SetLocation("Seattle");
            await p3.SetLocation("San Fransisco");

            var locIdx = await base.GetAndWaitForIndex<string, IPlayer1GrainNonFaultTolerant>("__Location");

            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayer1GrainNonFaultTolerant, Player1PropertiesNonFaultTolerant>("Seattle"));

            await p2.Deactivate();
            Thread.Sleep(1000);
            Assert.Equal(1, await this.CountPlayersStreamingIn<IPlayer1GrainNonFaultTolerant, Player1PropertiesNonFaultTolerant>("Seattle"));

            p2 = base.GetGrain<IPlayer1GrainNonFaultTolerant>(2);
            Assert.Equal("Seattle", await p2.GetLocation());
            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayer1GrainNonFaultTolerant, Player1PropertiesNonFaultTolerant>("Seattle"));
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
