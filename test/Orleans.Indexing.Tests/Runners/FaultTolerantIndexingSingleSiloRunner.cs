using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Indexing.Tests
{
    public abstract class FaultTolerantIndexingSingleSiloRunner : IndexingTestRunnerBase
    {
        protected FaultTolerantIndexingSingleSiloRunner(BaseIndexingFixture fixture, ITestOutputHelper output)
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

            IPlayer2Grain p1 = base.GetGrain<IPlayer2Grain>(1);
            await p1.SetLocation("Seattle");

            IPlayer2Grain p2 = base.GetGrain<IPlayer2Grain>(2);
            IPlayer2Grain p3 = base.GetGrain<IPlayer2Grain>(3);

            await p2.SetLocation("Seattle");
            await p3.SetLocation("San Fransisco");

            var locIdx = await base.GetAndWaitForIndex<string, IPlayer2Grain>("__Location");

            base.Output.WriteLine("Before check 1");
            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayer2Grain, Player2Properties>("Seattle", DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY));

            await p2.Deactivate();
            await Task.Delay(DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY);

            base.Output.WriteLine("Before check 2");
            Assert.Equal(1, await this.CountPlayersStreamingIn<IPlayer2Grain, Player2Properties>("Seattle", DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY));

            p2 = base.GetGrain<IPlayer2Grain>(2);
            base.Output.WriteLine("Before check 3");
            Assert.Equal("Seattle", await p2.GetLocation());

            base.Output.WriteLine("Before check 4");
            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayer2Grain, Player2Properties>("Seattle", DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY));
            base.Output.WriteLine("Done.");
        }
    }
}
