using Xunit.Abstractions;
using Xunit;

namespace Orleans.Indexing.Tests
{
    [TestCategory("BVT"), TestCategory("Indexing")]
    public class SimpleIndexingSingleSiloTestsWf : SimpleIndexingSingleSiloRunner, IClassFixture<WorkflowIndexingFixture>
    {
        public SimpleIndexingSingleSiloTestsWf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class SimpleIndexingTwoSiloTestsWf : SimpleIndexingTwoSiloRunner, IClassFixture<WorkflowIndexingFixture>
    {
        public SimpleIndexingTwoSiloTestsWf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class ActiveGrainEnumerationTestsWf : ActiveGrainEnumerationRunner, IClassFixture<WorkflowIndexingFixture>
    {
        public ActiveGrainEnumerationTestsWf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class ChainedBucketIndexingSingleSiloTestsWf : ChainedBucketIndexingSingleSiloRunner, IClassFixture<WorkflowIndexingFixture>
    {
        public ChainedBucketIndexingSingleSiloTestsWf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class ChainedBucketIndexingTwoSiloTestsWf : ChainedBucketIndexingTwoSiloRunner, IClassFixture<WorkflowIndexingFixture>
    {
        public ChainedBucketIndexingTwoSiloTestsWf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class FaultTolerantIndexingSingleSiloTestsWf : FaultTolerantIndexingSingleSiloRunner, IClassFixture<WorkflowIndexingFixture>
    {
        public FaultTolerantIndexingSingleSiloTestsWf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class FaultTolerantIndexingTwoSiloTestsWf : FaultTolerantIndexingTwoSiloRunner, IClassFixture<WorkflowIndexingFixture>
    {
        public FaultTolerantIndexingTwoSiloTestsWf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class LazyIndexingSingleSiloTestsWf : LazyIndexingSingleSiloRunner, IClassFixture<WorkflowIndexingFixture>
    {
        public LazyIndexingSingleSiloTestsWf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class LazyIndexingTwoSiloTestsWf : LazyIndexingTwoSiloRunner, IClassFixture<WorkflowIndexingFixture>
    {
        public LazyIndexingTwoSiloTestsWf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }

    [TestCategory("BVT"), TestCategory("Indexing")]
    public class NoIndexingTestsWf : NoIndexingRunner, IClassFixture<WorkflowIndexingFixture>
    {
        public NoIndexingTestsWf(WorkflowIndexingFixture fixture, ITestOutputHelper output) : base(fixture, output) { }
    }
}
