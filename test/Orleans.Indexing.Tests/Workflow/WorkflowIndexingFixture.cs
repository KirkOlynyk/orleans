using Orleans.TestingHost;
using Orleans.Hosting;
using Microsoft.Extensions.Logging;

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
                hostBuilder.UseIndexing(indexingOptions => indexingOptions.UseTransactions = false)
                           .ConfigureLogging(loggingBuilder => {
                               loggingBuilder.SetMinimumLevel(LogLevel.Information);
                               loggingBuilder.AddDebug();
                           })
                           .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(WorkflowIndexingFixture).Assembly));
            }
        }
    }
}
