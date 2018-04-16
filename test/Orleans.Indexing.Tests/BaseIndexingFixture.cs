using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.TestingHost;
using TestExtensions;
using UnitTests.GrainInterfaces;
using UnitTests.Grains;

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
                              .AddMemoryGrainStorage("PubSubStore") // PubSubStore service is run for silo startup
                              .AddMemoryGrainStorage(IndexingConstants.INDEXING_WORKFLOWQUEUE_STORAGE_PROVIDER_NAME)
                              .AddMemoryGrainStorage(IndexingConstants.INDEXING_STORAGE_PROVIDER_NAME)
                              .AddSimpleMessageStreamProvider(IndexingConstants.INDEXING_STREAM_PROVIDER_NAME)
                              .ConfigureLogging(loggingBuilder =>
                              {
                                  loggingBuilder.SetMinimumLevel(LogLevel.Information);
                                  loggingBuilder.AddDebug();
                              })
                              .ConfigureApplicationParts(parts =>
                              {
                                  parts.AddApplicationPart(typeof(BaseIndexingFixture).Assembly);
                                  parts.AddApplicationPart(typeof(ISimpleGrain).Assembly);
                                  parts.AddApplicationPart(typeof(SimpleGrain).Assembly);
                              });
        }

        internal static IClientBuilder Configure(IClientBuilder clientBuilder)
        {
            return clientBuilder.AddSimpleMessageStreamProvider(IndexingConstants.INDEXING_STREAM_PROVIDER_NAME)
                                .ConfigureLogging(loggingBuilder =>
                                {
                                    loggingBuilder.SetMinimumLevel(LogLevel.Information);
                                    loggingBuilder.AddDebug();
                                })
                                .ConfigureApplicationParts(parts =>
                                {
                                    parts.AddApplicationPart(typeof(BaseIndexingFixture).Assembly);
                                    parts.AddApplicationPart(typeof(ISimpleGrain).Assembly);
                                    parts.AddApplicationPart(typeof(SimpleGrain).Assembly);
                                });
        }
    }
}
