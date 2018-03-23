using Orleans.Runtime.Configuration;
using Orleans.TestingHost;
using TestExtensions;

namespace Orleans.Indexing.Tests
{
    public abstract class BaseIndexingFixture : BaseTestClusterFixture
    {
        protected TestClusterBuilder ConfigureTestClusterForIndexing(TestClusterBuilder builder)
        {
            builder.ConfigureLegacyConfiguration(legacy =>
            {
                legacy.ClusterConfiguration.AddMemoryStorageProvider(IndexingTestConstants.GrainStore);
                legacy.ClusterConfiguration.AddMemoryStorageProvider(IndexingTestConstants.MemoryStore);
            });
            return builder;
        }
    }
}
