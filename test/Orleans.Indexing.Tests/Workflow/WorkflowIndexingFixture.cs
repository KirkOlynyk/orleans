using Orleans.TestingHost;
using Orleans.Hosting;
using Microsoft.Extensions.Configuration;

namespace Orleans.Indexing.Tests
{
    public class WorkflowIndexingFixture : BaseIndexingFixture
    {
        protected override void ConfigureTestCluster(TestClusterBuilder builder)
        {
            base.ConfigureTestClusterForIndexing(builder)
                .AddSiloBuilderConfigurator<SiloBuilderConfiguratorWf>();
            builder.AddClientBuilderConfigurator<ClientBuilderConfiguratorWf>();
        }

        private class SiloBuilderConfiguratorWf : ISiloBuilderConfigurator
        {
            public void Configure(ISiloHostBuilder hostBuilder)
            {
                BaseIndexingFixture.Configure(hostBuilder)
                                   .UseIndexing(indexingOptions => ConfigureBasicOptions(indexingOptions));
            }
        }

        private class ClientBuilderConfiguratorWf : IClientBuilderConfigurator
        {
            public void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
            {
                BaseIndexingFixture.Configure(clientBuilder)
                                   .UseIndexing(indexingOptions => ConfigureBasicOptions(indexingOptions));
            }
        }

        private static IndexingOptions ConfigureBasicOptions(IndexingOptions indexingOptions)
        {
            indexingOptions.MaxHashBuckets = 42;
            indexingOptions.ConfigureWorkflow();
            return indexingOptions; // allow chaining
        }
    }
}
