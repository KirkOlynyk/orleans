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
                                   .UseIndexing(indexingOptions => indexingOptions.UseTransactions = false);
            }
        }

        private class ClientBuilderConfiguratorWf : IClientBuilderConfigurator
        {
            public void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
            {
                BaseIndexingFixture.Configure(clientBuilder)
                                   .UseIndexing(indexingOptions => indexingOptions.UseTransactions = false);
            }
        }
    }
}
