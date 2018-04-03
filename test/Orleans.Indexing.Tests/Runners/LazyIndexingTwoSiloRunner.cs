using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Indexing.Tests
{
    public abstract class LazyIndexingTwoSiloRunner : IndexingTestRunnerBase
    {
        protected LazyIndexingTwoSiloRunner(BaseIndexingFixture fixture, ITestOutputHelper output)
            : base(fixture, output)
        {
        }

        private const int DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY = 1000; //one second delay for writes to the in-memory indexes should be enough

        /// <summary>
        /// Tests basic functionality of ActiveHashIndexPartitionedPerSiloImpl with 2 Silos
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexLookup3()
        {
            await base.StartAndWaitForSecondSilo();

            IPlayer2GrainNonFaultTolerantLazy p1 = base.GetGrain<IPlayer2GrainNonFaultTolerantLazy>(1);
            await p1.SetLocation("Seattle");

            IPlayer2GrainNonFaultTolerantLazy p2 = base.GetGrain<IPlayer2GrainNonFaultTolerantLazy>(2);
            IPlayer2GrainNonFaultTolerantLazy p3 = base.GetGrain<IPlayer2GrainNonFaultTolerantLazy>(3);

            await p2.SetLocation("Seattle");
            await p3.SetLocation("San Fransisco");

            var locIdx = await base.GetAndWaitForIndex<string, IPlayer2GrainNonFaultTolerantLazy>("__Location");

            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayer2GrainNonFaultTolerantLazy, Player2PropertiesNonFaultTolerantLazy>("Seattle", DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY));

            await p2.Deactivate();
            await Task.Delay(DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY);

            Assert.Equal(1, await this.CountPlayersStreamingIn<IPlayer2GrainNonFaultTolerantLazy, Player2PropertiesNonFaultTolerantLazy>("Seattle", DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY));

            p2 = base.GetGrain<IPlayer2GrainNonFaultTolerantLazy>(2);
            Assert.Equal("Seattle", await p2.GetLocation());

            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayer2GrainNonFaultTolerantLazy, Player2PropertiesNonFaultTolerantLazy>("Seattle", DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY));
        }
    }
}
