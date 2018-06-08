using Orleans.Providers;
using System;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Indexing.Tests
{
    using ITC = IndexingTestConstants;

    public abstract class MultipleUniqueAndNonUniqueRunner: IndexingTestRunnerBase
    {
        protected MultipleUniqueAndNonUniqueRunner(BaseIndexingFixture fixture, ITestOutputHelper output)
            : base(fixture, output)
        {
        }

        [System.Serializable]
        public class Props_UIUSNINS_AI_UQ_LZ_PK : ITestIndexProperties
        {
            [Index(typeof(ActiveHashIndexPartitionedPerKey<int, IGrain_UIUSNINS_AI_UQ_LZ_PK>), IsEager = false, IsUnique = true, NullValue = "42")]
            public int UniqueInt { get; set; }

            [Index(typeof(ActiveHashIndexPartitionedPerKey<string, IGrain_UIUSNINS_AI_UQ_LZ_PK>), IsEager = false, IsUnique = true)]
            public string UniqueString { get; set; }

            [Index(typeof(ActiveHashIndexPartitionedPerKey<int, IGrain_UIUSNINS_AI_UQ_LZ_PK>), IsEager = false, IsUnique = false, NullValue = "24")]
            public int NonUniqueInt { get; set; }

            [Index(typeof(ActiveHashIndexPartitionedPerKey<string, IGrain_UIUSNINS_AI_UQ_LZ_PK>), IsEager = false, IsUnique = false)]
            public string NonUniqueString { get; set; }
        }

        [Serializable]
        public class State_UIUSNINS_AI_UQ_LZ_PK : Props_UIUSNINS_AI_UQ_LZ_PK, ITestIndexState
        {
            public string UnIndexedString { get; set; }
        }

        public interface IGrain_UIUSNINS_AI_UQ_LZ_PK : ITestIndexGrain, IIndexableGrain<Props_UIUSNINS_AI_UQ_LZ_PK>
        {
        }

        [StorageProvider(ProviderName = IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
        public class FT_Grain_UIUSNINS_AI_UQ_LZ_PK : TestIndexGrain<State_UIUSNINS_AI_UQ_LZ_PK, Props_UIUSNINS_AI_UQ_LZ_PK>,
                                                     IGrain_UIUSNINS_AI_UQ_LZ_PK
        {
        }

        [StorageProvider(ProviderName = IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
        public class NFT_Grain_UIUSNINS_AI_UQ_LZ_PK : TestIndexGrainNonFaultTolerant<State_UIUSNINS_AI_UQ_LZ_PK, Props_UIUSNINS_AI_UQ_LZ_PK>,
                                                     IGrain_UIUSNINS_AI_UQ_LZ_PK
        {
        }

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_UIUSNINS_AI_UQ_LZ_PK_FT()
        {
            Task<IGrain_UIUSNINS_AI_UQ_LZ_PK> makeGrain(int uInt, string uString, int nuInt, string nuString)
                => this.CreateGrain<IGrain_UIUSNINS_AI_UQ_LZ_PK, FT_Grain_UIUSNINS_AI_UQ_LZ_PK>(uInt, uString, nuInt, nuString);
            var p1 = await makeGrain(1, "one", 1000, "1k");
            var p11 = await makeGrain(11, "eleven", 1000, "1k");
            var p2 = await makeGrain(2, "two", 2000, "2k");
            var p3 = await makeGrain(3, "three", 3000, "3k");

            var intIdexes = base.GetAndWaitForIndexes<int, IGrain_UIUSNINS_AI_UQ_LZ_PK>(ITC.UniqueIntIndex, ITC.NonUniqueIntIndex);
            var stringIndexes = base.GetAndWaitForIndexes<string, IGrain_UIUSNINS_AI_UQ_LZ_PK>(ITC.UniqueStringIndex, ITC.NonUniqueStringIndex);

            async Task verifyCount(int expected1, int expected1000)
            {
                Assert.Equal(expected1, await this.GetUniqueIntCount<IGrain_UIUSNINS_AI_UQ_LZ_PK, Props_UIUSNINS_AI_UQ_LZ_PK>(1));
                Assert.Equal(expected1000, await this.GetNonUniqueIntCount<IGrain_UIUSNINS_AI_UQ_LZ_PK, Props_UIUSNINS_AI_UQ_LZ_PK>(1000));
            }

            await verifyCount(1, 2);

            await p11.Deactivate(ITC.DelayUntilIndexesAreUpdatedLazily);
            await verifyCount(1, 1);

            p11 = base.GetGrain<IGrain_UIUSNINS_AI_UQ_LZ_PK, FT_Grain_UIUSNINS_AI_UQ_LZ_PK>(p11.GetPrimaryKeyLong());
            Assert.Equal(1000, await p11.GetNonUniqueInt());
            await verifyCount(1, 2);
        }

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_UIUSNINS_AI_UQ_LZ_PK_NFT()
        {
            Task<IGrain_UIUSNINS_AI_UQ_LZ_PK> makeGrain(int ui, string us, int nui, string nus)
                => this.CreateGrain<IGrain_UIUSNINS_AI_UQ_LZ_PK, NFT_Grain_UIUSNINS_AI_UQ_LZ_PK>(ui, us, nui, nus);
            var p1 = await makeGrain(1, "one", 1000, "1k");
            var p11 = await makeGrain(11, "eleven", 1000, "1k");
            var p2 = await makeGrain(2, "two", 2000, "2k");
            var p3 = await makeGrain(3, "three", 3000, "3k");

            var intIdexes = base.GetAndWaitForIndexes<int, IGrain_UIUSNINS_AI_UQ_LZ_PK>(ITC.UniqueIntIndex, ITC.NonUniqueIntIndex);
            var stringIndexes = base.GetAndWaitForIndexes<string, IGrain_UIUSNINS_AI_UQ_LZ_PK>(ITC.UniqueStringIndex, ITC.NonUniqueStringIndex);

            async Task verifyCount(int expected1, int expected1000)
            {
                Assert.Equal(expected1, await this.GetUniqueIntCount<IGrain_UIUSNINS_AI_UQ_LZ_PK, Props_UIUSNINS_AI_UQ_LZ_PK>(1));
                Assert.Equal(expected1000, await this.GetNonUniqueIntCount<IGrain_UIUSNINS_AI_UQ_LZ_PK, Props_UIUSNINS_AI_UQ_LZ_PK>(1000));
            }

            await verifyCount(1, 2);

            await p11.Deactivate(ITC.DelayUntilIndexesAreUpdatedLazily);
            await verifyCount(1, 1);

            p11 = base.GetGrain<IGrain_UIUSNINS_AI_UQ_LZ_PK, FT_Grain_UIUSNINS_AI_UQ_LZ_PK>(p11.GetPrimaryKeyLong());
            Assert.Equal(1000, await p11.GetNonUniqueInt());
            await verifyCount(1, 2);
        }
    }
}
