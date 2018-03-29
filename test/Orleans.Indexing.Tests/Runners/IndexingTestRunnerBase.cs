using Orleans.Streams;
using Orleans.Runtime;
using Xunit.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Indexing.Tests
{
    public class IndexingTestRunnerBase
    {
        private BaseIndexingFixture fixture;

        internal readonly ITestOutputHelper Output;
        internal IClusterClient ClusterClient => this.fixture.Client;

        internal IGrainFactory GrainFactory => this.fixture.GrainFactory;

        internal IStreamProvider IndexingStreamProvider;

        internal ILoggerFactory LoggerFactory { get; }

        protected IndexingTestRunnerBase(BaseIndexingFixture fixture, ITestOutputHelper output)
        {
            this.fixture = fixture;
            this.Output = output;
            this.LoggerFactory = this.ClusterClient.ServiceProvider.GetRequiredService<ILoggerFactory>();
            this.IndexingStreamProvider = this.ClusterClient.ServiceProvider.GetRequiredServiceByName<IStreamProvider>(IndexingConstants.INDEXING_STREAM_PROVIDER_NAME);
        }

        protected T GetGrain<T>(long primaryKey) where T: IGrainWithIntegerKey
            => this.GrainFactory.GetGrain<T>(primaryKey);

        protected IIndexInterface<TKey, TValue> GetIndex<TKey, TValue>(string indexName) where TValue: IIndexableGrain
            => this.GrainFactory.GetIndex<TKey, TValue>(indexName);
    }
}
