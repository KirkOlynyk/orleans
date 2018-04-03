using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Indexing.Tests
{
    public abstract class LazyIndexingSingleSiloRunner : IndexingTestRunnerBase
    {
        protected LazyIndexingSingleSiloRunner(BaseIndexingFixture fixture, ITestOutputHelper output)
            : base(fixture, output)
        {
        }

        private const int DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY = 1000; //one second delay for writes to the in-memory indexes should be enough

        /// <summary>
        /// Tests basic functionality of HashIndexSingleBucker
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexLookup1()
        {
            IPlayer1GrainNonFaultTolerantLazy p1 = base.GetGrain<IPlayer1GrainNonFaultTolerantLazy>(1);
            await p1.SetLocation("Seattle");

            IPlayer1GrainNonFaultTolerantLazy p2 = base.GetGrain<IPlayer1GrainNonFaultTolerantLazy>(2);
            IPlayer1GrainNonFaultTolerantLazy p3 = base.GetGrain<IPlayer1GrainNonFaultTolerantLazy>(3);

            await p2.SetLocation("Seattle");
            await p3.SetLocation("San Fransisco");

            var locIdx = await base.GetAndWaitForIndex<string, IPlayer1GrainNonFaultTolerantLazy>("__Location");

            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayer1GrainNonFaultTolerantLazy, Player1PropertiesNonFaultTolerantLazy>("Seattle", DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY));

            await p2.Deactivate();
            await Task.Delay(DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY);

            Assert.Equal(1, await this.CountPlayersStreamingIn<IPlayer1GrainNonFaultTolerantLazy, Player1PropertiesNonFaultTolerantLazy>("Seattle", DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY));

            p2 = base.GetGrain<IPlayer1GrainNonFaultTolerantLazy>(2);
            Assert.Equal("Seattle", await p2.GetLocation());

            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayer1GrainNonFaultTolerantLazy, Player1PropertiesNonFaultTolerantLazy>("Seattle", DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY));
        }

        /// <summary>
        /// Tests basic functionality of ActiveHashIndexPartitionedPerSiloImpl with 1 Silo
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexLookup2()
        {
            IPlayer2GrainNonFaultTolerantLazy p1 = base.GetGrain<IPlayer2GrainNonFaultTolerantLazy>(1);
            await p1.SetLocation("Tehran");

            IPlayer2GrainNonFaultTolerantLazy p2 = base.GetGrain<IPlayer2GrainNonFaultTolerantLazy>(2);
            IPlayer2GrainNonFaultTolerantLazy p3 = base.GetGrain<IPlayer2GrainNonFaultTolerantLazy>(3);

            await p2.SetLocation("Tehran");
            await p3.SetLocation("Yazd");

            var locIdx = await base.GetAndWaitForIndex<string, IPlayer2GrainNonFaultTolerantLazy>("__Location");

            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayer2GrainNonFaultTolerantLazy, Player2PropertiesNonFaultTolerantLazy>("Tehran", DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY));

            await p2.Deactivate();
            await Task.Delay(DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY);

            Assert.Equal(1, await this.CountPlayersStreamingIn<IPlayer2GrainNonFaultTolerantLazy, Player2PropertiesNonFaultTolerantLazy>("Tehran", DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY));

            p2 = base.GetGrain<IPlayer2GrainNonFaultTolerantLazy>(2);
            Assert.Equal("Tehran", await p2.GetLocation());

            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayer2GrainNonFaultTolerantLazy, Player2PropertiesNonFaultTolerantLazy>("Tehran", DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY));
        }

        /// <summary>
        /// Tests basic functionality of HashIndexPartitionedPerKey
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexLookup4()
        {
            IPlayer3GrainNonFaultTolerantLazy p1 = base.GetGrain<IPlayer3GrainNonFaultTolerantLazy>(1);
            await p1.SetLocation("Seattle");

            IPlayer3GrainNonFaultTolerantLazy p2 = base.GetGrain<IPlayer3GrainNonFaultTolerantLazy>(2);
            IPlayer3GrainNonFaultTolerantLazy p3 = base.GetGrain<IPlayer3GrainNonFaultTolerantLazy>(3);

            await p2.SetLocation("Seattle");
            await p3.SetLocation("San Fransisco");

            var locIdx = await base.GetAndWaitForIndex<string, IPlayer3GrainNonFaultTolerantLazy>("__Location");

            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayer3GrainNonFaultTolerantLazy, Player3PropertiesNonFaultTolerantLazy>("Seattle", DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY));

            await p2.Deactivate();
            await Task.Delay(DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY);

            Assert.Equal(1, await this.CountPlayersStreamingIn<IPlayer3GrainNonFaultTolerantLazy, Player3PropertiesNonFaultTolerantLazy>("Seattle", DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY));

            p2 = base.GetGrain<IPlayer3GrainNonFaultTolerantLazy>(2);
            Assert.Equal("Seattle", await p2.GetLocation());

            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayer3GrainNonFaultTolerantLazy, Player3PropertiesNonFaultTolerantLazy>("Seattle", DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY));
        }
    }
}
