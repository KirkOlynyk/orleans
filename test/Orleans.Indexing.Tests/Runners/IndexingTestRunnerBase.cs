using Xunit.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Orleans.TestingHost;
using System.Threading.Tasks;
using System.Threading;
using System.Linq;
using System;
using Xunit;

namespace Orleans.Indexing.Tests
{
    using ITC = IndexingTestConstants;

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

        public async Task<TInterface> CreateGrain<TInterface>(int uInt, string uString, int nuInt, string nuString) where TInterface : IGrainWithIntegerKey, ITestIndexGrain
        {
            var p1 = this.GetGrain<TInterface>(GrainPkFromUniqueInt(uInt));
            await p1.SetUniqueInt(uInt);
            await p1.SetUniqueString(uString);
            await p1.SetNonUniqueInt(nuInt);
            await p1.SetNonUniqueString(nuString);
            return p1;
        }

        protected async Task TestIndexesWithOneDeactivation<TIGrain, TProperties>()
            where TIGrain : ITestIndexGrain, IIndexableGrain where TProperties : ITestIndexProperties
        {
            using (var tw = new TestConsoleOutputWriter(this.Output, "start test"))
            {
                Task<TIGrain> makeGrain(int uInt, string uString, int nuInt, string nuString)
                    => this.CreateGrain<TIGrain>(uInt, uString, nuInt, nuString);
                var p1 = await makeGrain(1, "one", 1000, "1k");
                var p11 = await makeGrain(11, "eleven", 1000, "1k");
                var p2 = await makeGrain(2, "two", 2000, "2k");
                var p3 = await makeGrain(3, "three", 3000, "3k");

                var intIndexes = await this.GetAndWaitForIndexes<int, TIGrain>(ITC.UniqueIntIndex, ITC.NonUniqueIntIndex);
                var nuIntIndex = intIndexes[1];
                bool isNuIntTotalIndex = typeof(ITotalIndex).IsAssignableFrom(nuIntIndex.GetType());
                var stringIndexes = await this.GetAndWaitForIndexes<string, TIGrain>(ITC.UniqueStringIndex, ITC.NonUniqueStringIndex);

                await Task.Delay(ITC.DelayUntilIndexesAreUpdatedLazily);

                Assert.Equal(1, await this.GetUniqueIntCount<TIGrain, TProperties>(2));
                Assert.Equal(1, await this.GetUniqueIntCount<TIGrain, TProperties>(3));
                Assert.Equal(1, await this.GetUniqueStringCount<TIGrain, TProperties>("two"));
                Assert.Equal(1, await this.GetUniqueStringCount<TIGrain, TProperties>("three"));
                Assert.Equal(1, await this.GetNonUniqueIntCount<TIGrain, TProperties>(2000));
                Assert.Equal(1, await this.GetNonUniqueIntCount<TIGrain, TProperties>(3000));
                Assert.Equal(1, await this.GetNonUniqueStringCount<TIGrain, TProperties>("2k"));
                Assert.Equal(1, await this.GetNonUniqueStringCount<TIGrain, TProperties>("3k"));

                async Task verifyCount(int expected1, int expected11, int expected1000)
                {
                    Assert.Equal(expected1, await this.GetUniqueIntCount<TIGrain, TProperties>(1));
                    Assert.Equal(expected11, await this.GetUniqueIntCount<TIGrain, TProperties>(11));
                    Assert.Equal(expected1000, await this.GetNonUniqueIntCount<TIGrain, TProperties>(1000));
                    Assert.Equal(expected1000, await this.GetNonUniqueStringCount<TIGrain, TProperties>("1k"));
                }

                Console.WriteLine("*** First Verify ***");
                await verifyCount(1, 1, 2);

                Console.WriteLine("*** Deactivate ***");
                await p11.Deactivate(ITC.DelayUntilIndexesAreUpdatedLazily);
                var numGone = isNuIntTotalIndex ? 0 : 1;

                Console.WriteLine("*** Second Verify ***");
                await verifyCount(1, 1 - numGone, 2 - numGone);

                Console.WriteLine("*** GetGrain ***");
                p11 = this.GetGrain<TIGrain>(p11.GetPrimaryKeyLong());
                Assert.Equal(1000, await p11.GetNonUniqueInt());
                Console.WriteLine("*** Third Verify ***");
                await verifyCount(1, 1, 2);
            }
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
