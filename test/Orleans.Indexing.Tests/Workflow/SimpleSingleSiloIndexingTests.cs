using Xunit.Abstractions;
using Xunit;

namespace Orleans.Indexing.Tests
{
    [TestCategory("BVT"), TestCategory("Indexing")]
    public class SimpleSingleSiloIndexingTestsWf : SimpleSingleSiloIndexingRunner, IClassFixture<WorkflowIndexingFixture>
    {
        public SimpleSingleSiloIndexingTestsWf(WorkflowIndexingFixture fixture, ITestOutputHelper output)
            : base(fixture.GrainFactory, fixture.Client, output)
        {
        }
    }
}
