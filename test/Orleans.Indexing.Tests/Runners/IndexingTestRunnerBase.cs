using Xunit.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Orleans.TestingHost;
using System.Threading.Tasks;
using System.Threading;
using Orleans.Runtime;

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

        internal IndexingTestSchedulingContext OutsideSchedulingContext = new IndexingTestSchedulingContext();

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

    internal class IndexingTestSchedulingContext : ISchedulingContext
    {
        public SchedulingContextType ContextType => SchedulingContextType.Activation;

        public string Name => this.GetType().Name;

        public bool IsSystemPriorityContext => false;

        public string DetailedStatus() => this.ToString();

        #region IEquatable<ISchedulingContext> Members

        public bool Equals(ISchedulingContext other) => base.Equals(other);

        #endregion
    }
}
