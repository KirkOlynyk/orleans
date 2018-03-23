using Orleans.Streams;
using Orleans.Runtime;
using Orleans.Indexing;
using Xunit.Abstractions;

namespace Orleans.Indexing.Tests
{
    public class IndexingTestRunnerBase
    {
        internal readonly ITestOutputHelper Output;
        private IClusterClient clusterClient;

        internal IGrainFactory GrainFactory { get; private set; }

        internal IStreamProvider IndexingStreamProvider
                => this.clusterClient.ServiceProvider.GetRequiredServiceByName<IStreamProvider>(IndexingConstants.INDEXING_STREAM_PROVIDER_NAME);

        protected IndexingTestRunnerBase(IGrainFactory grainFactory, IClusterClient clusterClient, ITestOutputHelper output)
        {
            this.Output = output;
            this.GrainFactory = grainFactory;
            this.clusterClient = clusterClient;
        }

        protected T GetGrain<T>(long primaryKey) where T: IGrainWithIntegerKey
        {
            return this.GrainFactory.GetGrain<T>(primaryKey);
        }

        protected IIndexInterface<TKey, TValue> GetIndex<TKey, TValue>(string indexName) where TValue: IIndexableGrain
        {
            return this.GrainFactory.GetIndex<TKey, TValue>(indexName);
        }
    }
}
