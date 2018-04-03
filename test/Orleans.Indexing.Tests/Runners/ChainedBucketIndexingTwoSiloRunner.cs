using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Indexing.Tests
{
    public abstract class ChainedBucketIndexingTwoSiloRunner : IndexingTestRunnerBase
    {
        protected ChainedBucketIndexingTwoSiloRunner(BaseIndexingFixture fixture, ITestOutputHelper output)
            : base(fixture, output)
        {
        }

        /// <summary>
        /// Tests basic functionality of ActiveHashIndexPartitionedPerSiloImpl with 2 Silos
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexLookup3()
        {
            await base.StartAndWaitForSecondSilo();

            IPlayer2GrainNonFaultTolerant p1 = base.GetGrain<IPlayer2GrainNonFaultTolerant>(1);
            await p1.SetLocation("Seattle");

            IPlayer2GrainNonFaultTolerant p2 = base.GetGrain<IPlayer2GrainNonFaultTolerant>(2);
            IPlayer2GrainNonFaultTolerant p3 = base.GetGrain<IPlayer2GrainNonFaultTolerant>(3);

            await p2.SetLocation("Seattle");
            await p3.SetLocation("San Fransisco");

            var locIdx = await base.GetAndWaitForIndex<string, IPlayer2GrainNonFaultTolerant>("__Location");

            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayer2GrainNonFaultTolerant, Player2PropertiesNonFaultTolerant>("Seattle"));

            await p2.Deactivate();
            Thread.Sleep(1000);

            Assert.Equal(1, await this.CountPlayersStreamingIn<IPlayer2GrainNonFaultTolerant, Player2PropertiesNonFaultTolerant>("Seattle"));

            p2 = base.GetGrain<IPlayer2GrainNonFaultTolerant>(2);
            Assert.Equal("Seattle", await p2.GetLocation());

            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayer2GrainNonFaultTolerant, Player2PropertiesNonFaultTolerant>("Seattle"));
        }
    }
}
