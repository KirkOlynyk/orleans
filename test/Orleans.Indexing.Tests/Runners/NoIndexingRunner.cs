using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Indexing.Tests
{
    public abstract class NoIndexingRunner : IndexingTestRunnerBase
    {
        protected NoIndexingRunner(BaseIndexingFixture fixture, ITestOutputHelper output)
            : base(fixture, output)
        {
        }

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_NoIndex()
        {
            IPlayer1GrainNonFaultTolerant p100 = base.GetGrain<IPlayer1GrainNonFaultTolerant>(100);
            IPlayer1GrainNonFaultTolerant p200 = base.GetGrain<IPlayer1GrainNonFaultTolerant>(200);
            IPlayer1GrainNonFaultTolerant p300 = base.GetGrain<IPlayer1GrainNonFaultTolerant>(300);

            await p100.SetLocation("Tehran");
            await p200.SetLocation("Tehran");
            await p300.SetLocation("Yazd");

            Assert.Equal("Tehran", await p100.GetLocation());
            Assert.Equal("Tehran", await p200.GetLocation());
            Assert.Equal("Yazd", await p300.GetLocation());
        }
    }
}
