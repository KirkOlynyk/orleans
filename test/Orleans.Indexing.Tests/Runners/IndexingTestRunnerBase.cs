using Xunit.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Orleans.TestingHost;
using System.Threading.Tasks;
using System.Threading;
using Orleans.Runtime;
using System.Linq;
using System;

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

        protected TInterface GetGrain<TInterface>(long primaryKey) where TInterface : IGrainWithIntegerKey
            => this.GrainFactory.GetGrain<TInterface>(primaryKey);

        protected TInterface GetGrain<TInterface, TImplClass>(long primaryKey) where TInterface : IGrainWithIntegerKey
            => this.GetGrain<TInterface>(primaryKey, typeof(TImplClass));

        protected TInterface GetGrain<TInterface>(long primaryKey, Type grainImplType) where TInterface : IGrainWithIntegerKey
            => this.GrainFactory.GetGrain<TInterface>(primaryKey, grainImplType.FullName.Replace("+", "."));

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

        public async Task<TInterface> CreateGrain<TInterface, TImplClass>(int uInt, string uString, int nuInt, string nuString) where TInterface : IGrainWithIntegerKey, ITestIndexGrain
        {
            var p1 = this.GetGrain<TInterface>(GrainPkFromUniqueInt(uInt), typeof(TImplClass));
            await p1.SetUniqueInt(uInt);
            await p1.SetUniqueString(uString);
            await p1.SetNonUniqueInt(nuInt);
            await p1.SetNonUniqueString(nuString);
            return p1;
        }

        public static long GrainPkFromUniqueInt(int uInt) => uInt + 4200000000000;

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
