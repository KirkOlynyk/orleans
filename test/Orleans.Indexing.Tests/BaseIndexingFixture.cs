using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Runtime.Configuration;
using Orleans.TestingHost;
using TestExtensions;

namespace Orleans.Indexing.Tests
{
    public abstract class BaseIndexingFixture : BaseTestClusterFixture
    {
        protected TestClusterBuilder ConfigureTestClusterForIndexing(TestClusterBuilder builder)
        {
            // Currently nothing
            return builder;
        }

        internal static ISiloHostBuilder Configure(ISiloHostBuilder hostBuilder)
        {
            return hostBuilder.AddMemoryGrainStorage(IndexingTestConstants.GrainStore)
                              .AddMemoryGrainStorage(IndexingTestConstants.MemoryStore)
                              .ConfigureLogging(loggingBuilder =>
                              {
                                  loggingBuilder.SetMinimumLevel(LogLevel.Information);
                                  loggingBuilder.AddDebug();
                              })
                              .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(BaseIndexingFixture).Assembly));
        }
    }
}
