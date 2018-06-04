using Xunit.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Orleans.TestingHost;
using System.Threading.Tasks;
using System.Threading;
using Orleans.Runtime;
using System.Linq;

namespace Orleans.Indexing.Tests
{
    public class IndexingTestRunnerBase
    {
        private BaseIndexingFixture fixture;

        internal readonly ITestOutputHelper Output;
        internal IClusterClient ClusterClient => this.fixture.Client;

        internal IGrainFactory GrainFactory => this.fixture.GrainFactory;

        internal IIndexFactory IndexFactory { get; }

        internal ILoggerFactory LoggerFactory { get; }

        protected TestCluster HostedCluster => this.fixture.HostedCluster;

        protected IndexingTestRunnerBase(BaseIndexingFixture fixture, ITestOutputHelper output)
        {
            this.fixture = fixture;
            this.Output = output;
            this.LoggerFactory = this.ClusterClient.ServiceProvider.GetRequiredService<ILoggerFactory>();
            this.IndexFactory = this.ClusterClient.ServiceProvider.GetRequiredService<IIndexFactory>();
        }

        protected T GetGrain<T>(long primaryKey) where T : IGrainWithIntegerKey
            => this.GrainFactory.GetGrain<T>(primaryKey);

        protected IIndexInterface<TKey, TValue> GetIndex<TKey, TValue>(string indexName) where TValue : IIndexableGrain
            => this.IndexFactory.GetIndex<TKey, TValue>(indexName);

        protected async Task<IIndexInterface<TKey, TValue>> GetAndWaitForIndex<TKey, TValue>(string indexName) where TValue : IIndexableGrain
        {
            var locIdx = this.IndexFactory.GetIndex<TKey, TValue>(indexName);
            while (!await locIdx.IsAvailable()) Thread.Sleep(50);
            return locIdx;
        }

        protected async Task<IIndexInterface<TKey, TValue>[]> GetAndWaitForIndexes<TKey, TValue>(params string[] indexNames) where TValue : IIndexableGrain
        {
            var indexes = indexNames.Select(name => this.IndexFactory.GetIndex<TKey, TValue>(name)).ToArray();
            foreach (var index in indexes)
            {
                if (!await index.IsAvailable()) await Task.Delay(50);
            }
            return indexes;
        }

        public async Task<T> CreateGrain<T>(int uInt, string uString, int nuInt, string nuString) where T : IGrainWithIntegerKey, ITestIndexGrain
        {
            var p1 = this.GetGrain<T>(uInt + 4200000000000);
            await p1.SetUniqueInt(uInt);
            await p1.SetUniqueString(uString);
            await p1.SetNonUniqueInt(nuInt);
            await p1.SetUniqueString(nuString);
            return p1;
        }

        protected Task StartAndWaitForSecondSilo()
        {
            if (this.HostedCluster.SecondarySilos.Count == 0)
            {
                this.HostedCluster.StartAdditionalSilo();
                return this.HostedCluster.WaitForLivenessToStabilizeAsync();
            }
            return Task.CompletedTask;
        }
    }
}
