using Orleans.TestingHost;
using Orleans.Hosting;

namespace Orleans.Indexing.Tests
{
    public class WorkflowIndexingFixture : BaseIndexingFixture
    {
        protected override void ConfigureTestCluster(TestClusterBuilder builder)
        {
            base.ConfigureTestClusterForIndexing(builder)
                .AddSiloBuilderConfigurator<SiloBuilderConfigurator>();
        }

        private class SiloBuilderConfigurator : ISiloBuilderConfigurator
        {
            public void Configure(ISiloHostBuilder hostBuilder)
            {
                BaseIndexingFixture.Configure(hostBuilder).UseIndexing(indexingOptions => indexingOptions.UseTransactions = false);
            }
        }
    }
}
