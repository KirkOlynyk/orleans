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

            const int MaxRetries = 100;
            int retries = 0;
            foreach (var index in indexes)
            {
                while (!await index.IsAvailable())
                {
                    ++retries;
                    Assert.True(retries < MaxRetries, "Maximum number of GetAndWaitForIndexes retries was exceeded");
                    await Task.Delay(50);
                }
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

        internal async Task TestIndexesWithDeactivations<TIGrain, TProperties>(int intAdjust = 0)
            where TIGrain : ITestIndexGrain, IIndexableGrain where TProperties : ITestIndexProperties
        {
            using (var tw = new TestConsoleOutputWriter(this.Output, $"start test: TIGrain = {nameof(TIGrain)}, TProperties = {nameof(TProperties)}"))
            {
                // Use intAdjust to test that different values for the same grain type are handled correctly; see MultiIndex_All.
                var adj1 = intAdjust + 1;
                var adj11 = intAdjust + 11;
                var adj111 = intAdjust + 111;
                var adj1111 = intAdjust + 1111;
                var adj2 = intAdjust + 2;
                var adj3 = intAdjust + 3;
                var adj1000 = intAdjust + 1000;
                var adj2000 = intAdjust + 2000;
                var adj3000 = intAdjust + 3000;
                var adjOne = "one" + intAdjust;
                var adjEleven = "eleven" + intAdjust;
                var adjOneEleven = "oneeleven" + intAdjust;
                var adjElevenEleven = "eleveneleven" + intAdjust;
                var adjTwo = "two" + intAdjust;
                var adjThree = "three" + intAdjust;
                var adj1k = "1k" + intAdjust;
                var adj2k = "2k" + intAdjust;
                var adj3k = "3k" + intAdjust;

                Task<TIGrain> makeGrain(int uInt, string uString, int nuInt, string nuString)
                    => this.CreateGrain<TIGrain>(uInt, uString, nuInt, nuString);
                var p1 = await makeGrain(adj1, adjOne, adj1000, adj1k);
                var p11 = await makeGrain(adj11, adjEleven, adj1000, adj1k);
                var p111 = await makeGrain(adj111, adjOneEleven, adj1000, adj1k);
                var p1111 = await makeGrain(adj1111, adjElevenEleven, adj1000, adj1k);
                var p2 = await makeGrain(adj2, adjTwo, adj2000, adj2k);
                var p3 = await makeGrain(adj3, adjThree, adj3000, adj3k);

                var intIndexes = await this.GetAndWaitForIndexes<int, TIGrain>(ITC.UniqueIntIndex, ITC.NonUniqueIntIndex);
                var nonUniqueIntIndexType = intIndexes[1].GetType();
                bool ignoreDeactivate = typeof(ITotalIndex).IsAssignableFrom(nonUniqueIntIndexType)
                                      || typeof(IDirectStorageManagedIndex).IsAssignableFrom(nonUniqueIntIndexType);
                var stringIndexes = await this.GetAndWaitForIndexes<string, TIGrain>(ITC.UniqueStringIndex, ITC.NonUniqueStringIndex);

                Assert.Equal(1, await this.GetUniqueStringCount<TIGrain, TProperties>(adjOne));
                Assert.Equal(1, await this.GetUniqueStringCount<TIGrain, TProperties>(adjEleven));
                Assert.Equal(1, await this.GetUniqueIntCount<TIGrain, TProperties>(adj2));
                Assert.Equal(1, await this.GetUniqueIntCount<TIGrain, TProperties>(adj3));
                Assert.Equal(1, await this.GetUniqueStringCount<TIGrain, TProperties>(adjTwo));
                Assert.Equal(1, await this.GetUniqueStringCount<TIGrain, TProperties>(adjThree));
                Assert.Equal(1, await this.GetNonUniqueIntCount<TIGrain, TProperties>(adj2000));
                Assert.Equal(1, await this.GetNonUniqueIntCount<TIGrain, TProperties>(adj3000));
                Assert.Equal(1, await this.GetNonUniqueStringCount<TIGrain, TProperties>(adj2k));
                Assert.Equal(1, await this.GetNonUniqueStringCount<TIGrain, TProperties>(adj3k));

                async Task verifyCount(int expected1, int expected11, int expected1000)
                {
                    Assert.Equal(expected1, await this.GetUniqueIntCount<TIGrain, TProperties>(adj1));
                    Assert.Equal(expected11, await this.GetUniqueIntCount<TIGrain, TProperties>(adj11));
                    Assert.Equal(expected1000, await this.GetNonUniqueIntCount<TIGrain, TProperties>(adj1000));
                    Assert.Equal(expected1000, await this.GetNonUniqueStringCount<TIGrain, TProperties>(adj1k));
                }

                Console.WriteLine("*** First Verify ***");
                await verifyCount(1, 1, 4);

                Console.WriteLine("*** First Deactivate ***");
                await p11.Deactivate();
                await Task.Delay(ITC.DelayUntilIndexesAreUpdatedLazily);

                Console.WriteLine("*** Second Verify ***");
                await verifyCount(1, ignoreDeactivate ? 1 : 0, ignoreDeactivate ? 4 : 3);

                Console.WriteLine("*** Second and Third Deactivate ***");
                await p111.Deactivate();
                await p1111.Deactivate();
                await Task.Delay(ITC.DelayUntilIndexesAreUpdatedLazily);

                Console.WriteLine("*** Third Verify ***");
                await verifyCount(1, ignoreDeactivate ? 1 : 0, ignoreDeactivate ? 4 : 1);

                Console.WriteLine("*** GetGrain ***");
                p11 = this.GetGrain<TIGrain>(p11.GetPrimaryKeyLong());
                Assert.Equal(adj1000, await p11.GetNonUniqueInt());
                Console.WriteLine("*** Fourth Verify ***");
                await verifyCount(1, 1, ignoreDeactivate ? 4 : 2);
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
