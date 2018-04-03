using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Indexing.Tests
{
    public abstract class FaultTolerantIndexingTwoSiloRunner : IndexingTestRunnerBase
    {
        protected FaultTolerantIndexingTwoSiloRunner(BaseIndexingFixture fixture, ITestOutputHelper output)
            : base(fixture, output)
        {
        }

        private const int DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY = 1000; //one second delay for writes to the in-memory indexes should be enough

        /// <summary>
        /// Tests basic functionality of HashIndexSingleBucket
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexLookup1()
        {
            IPlayer1Grain p1 = base.GetGrain<IPlayer1Grain>(1);
            await p1.SetLocation("Seattle");

            IPlayer1Grain p2 = base.GetGrain<IPlayer1Grain>(2);
            IPlayer1Grain p3 = base.GetGrain<IPlayer1Grain>(3);

            await p2.SetLocation("Seattle");
            await p3.SetLocation("San Fransisco");

            var locIdx = await base.GetAndWaitForIndex<string, IPlayer1Grain>("__Location");

            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayer1Grain, Player1Properties>("Seattle", DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY));

            await p2.Deactivate();
            await Task.Delay(DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY);

            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayer1Grain, Player1Properties>("Seattle", DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY));

            p2 = base.GetGrain<IPlayer1Grain>(2);
            Assert.Equal("Seattle", await p2.GetLocation());

            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayer1Grain, Player1Properties>("Seattle", DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY));
        }

        /// <summary>
        /// Tests basic functionality of ActiveHashIndexPartitionedPerSiloImpl with 1 Silo
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexLookup2()
        {
            IPlayer2Grain p1 = base.GetGrain<IPlayer2Grain>(1);
            await p1.SetLocation("Tehran");

            IPlayer2Grain p2 = base.GetGrain<IPlayer2Grain>(2);
            IPlayer2Grain p3 = base.GetGrain<IPlayer2Grain>(3);

            await p2.SetLocation("Tehran");
            await p3.SetLocation("Yazd");

            var locIdx = await base.GetAndWaitForIndex<string, IPlayer2Grain>("__Location");

            base.Output.WriteLine("Before check 1");
            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayer2Grain, Player2Properties>("Tehran", DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY));

            await p2.Deactivate();

            await Task.Delay(DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY);

            base.Output.WriteLine("Before check 2");
            Assert.Equal(1, await this.CountPlayersStreamingIn<IPlayer2Grain, Player2Properties>("Tehran", DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY));

            p2 = base.GetGrain<IPlayer2Grain>(2);
            base.Output.WriteLine("Before check 3");
            Assert.Equal("Tehran", await p2.GetLocation());

            base.Output.WriteLine("Before check 4");
            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayer2Grain, Player2Properties>("Tehran", DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY));
            base.Output.WriteLine("Done.");
        }

        /// <summary>
        /// Tests basic functionality of HashIndexPartitionedPerKey
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexLookup4()
        {
            IPlayer3Grain p1 = base.GetGrain<IPlayer3Grain>(1);
            await p1.SetLocation("Seattle");

            IPlayer3Grain p2 = base.GetGrain<IPlayer3Grain>(2);
            IPlayer3Grain p3 = base.GetGrain<IPlayer3Grain>(3);

            await p2.SetLocation("Seattle");
            await p3.SetLocation("San Fransisco");

            var locIdx = await base.GetAndWaitForIndex<string, IPlayer3Grain>("__Location");

            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayer3Grain, Player3Properties>("Seattle", DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY));

            await p2.Deactivate();
            await Task.Delay(DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY);

            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayer3Grain, Player3Properties>("Seattle", DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY));

            p2 = base.GetGrain<IPlayer3Grain>(2);
            Assert.Equal("Seattle", await p2.GetLocation());

            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayer3Grain, Player3Properties>("Seattle", DELAY_UNTIL_INDEXES_ARE_UPDATED_LAZILY));
        }

        /// <summary>
        /// Tests basic functionality of HashIndexPartitionedPerKey
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexLookup5()
        {
            //await base.StartAndWaitForSecondSilo();

            IPlayer3Grain p1 = base.GetGrain<IPlayer3Grain>(1);
            await p1.SetLocation("Seattle");

            IPlayer3Grain p2 = base.GetGrain<IPlayer3Grain>(2);
            IPlayer3Grain p3 = base.GetGrain<IPlayer3Grain>(3);
            IPlayer3Grain p4 = base.GetGrain<IPlayer3Grain>(4);
            IPlayer3Grain p5 = base.GetGrain<IPlayer3Grain>(5);

            await p2.SetLocation("Seattle");
            await p3.SetLocation("San Fransisco");
            await p4.SetLocation("Tehran");
            await p5.SetLocation("Yazd");

            for(int i = 0; i < 100; ++i)
            {
                var tasks = new List<Task>();
                for (int j = 0; j < 10; ++j)
                {
                    p1 = base.GetGrain<IPlayer3Grain>(j);
                    tasks.Add(p1.SetLocation("Yazd" + i + "-" + j ));
                }
                await Task.WhenAll(tasks);
            }
        }
    }
}
