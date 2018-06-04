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
            [Index(typeof(ActiveHashIndexPartitionedPerKey<int, IGrain_UIUSNINS_AI_UQ_LZ_PK_FT>), IsEager = false, IsUnique = true)]
            public int UniqueInt { get; set; }

            [Index(typeof(ActiveHashIndexPartitionedPerKey<string, IGrain_UIUSNINS_AI_UQ_LZ_PK_FT>), IsEager = false, IsUnique = true)]
            public string UniqueString { get; set; }

            [Index(typeof(ActiveHashIndexPartitionedPerKey<int, IGrain_UIUSNINS_AI_UQ_LZ_PK_FT>), IsEager = false, IsUnique = false)]
            public int NonUniqueInt { get; set; }

            [Index(typeof(ActiveHashIndexPartitionedPerKey<string, IGrain_UIUSNINS_AI_UQ_LZ_PK_FT>), IsEager = false, IsUnique = false)]
            public string NonUniqueString { get; set; }
        }

        public interface IGrain_UIUSNINS_AI_UQ_LZ_PK_FT : ITestIndexGrain, IIndexableGrain<Props_UIUSNINS_AI_UQ_LZ_PK>
        {
        }

        [Serializable]
        public class State_UIUSNINS_AI_UQ_LZ_PK : Props_UIUSNINS_AI_UQ_LZ_PK, ITestIndexState
        {
            public string UnIndexedString { get; set; }
        }

        [StorageProvider(ProviderName = IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
        public class Grain_UIUSNINS_AI_UQ_LZ_PK_FT : TestIndexGrain<State_UIUSNINS_AI_UQ_LZ_PK, Props_UIUSNINS_AI_UQ_LZ_PK>,
                                                     IGrain_UIUSNINS_AI_UQ_LZ_PK_FT
        {
        }

        //TODO [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_UIUSNINS_AI_UQ_LZ_PK_NFT()
        {
            var p1 = await this.CreateGrain<IGrain_UIUSNINS_AI_UQ_LZ_PK_FT>(1, "one", 1000, "1k");
            var p11 = await this.CreateGrain<IGrain_UIUSNINS_AI_UQ_LZ_PK_FT>(11, "eleven", 1000, "1k");
            var p2 = await this.CreateGrain<IGrain_UIUSNINS_AI_UQ_LZ_PK_FT>(2, "two", 2000, "2k");
            var p3 = await this.CreateGrain<IGrain_UIUSNINS_AI_UQ_LZ_PK_FT>(3, "three", 3000, "3k");

            var intIdexes = base.GetAndWaitForIndexes<int, IGrain_UIUSNINS_AI_UQ_LZ_PK_FT>(ITC.UniqueIntIndex, ITC.NonUniqueIntIndex);
            var stringIndexes = base.GetAndWaitForIndexes<string, IGrain_UIUSNINS_AI_UQ_LZ_PK_FT>(ITC.UniqueStringIndex, ITC.NonUniqueStringIndex);

            async Task verifyCount(int expected1, int expected1000)
            {
                Assert.Equal(expected1, await this.GetUniqueIntCount<IGrain_UIUSNINS_AI_UQ_LZ_PK_FT, Props_UIUSNINS_AI_UQ_LZ_PK>(1));
                Assert.Equal(expected1000, await this.GetNonUniqueIntCount<IGrain_UIUSNINS_AI_UQ_LZ_PK_FT, Props_UIUSNINS_AI_UQ_LZ_PK>(1000));
            }

            await verifyCount(1, 2);

            await p11.Deactivate();
            await verifyCount(1, 1);

            p11 = base.GetGrain<IGrain_UIUSNINS_AI_UQ_LZ_PK_FT>(11);
            Assert.Equal(1000, await p11.GetNonUniqueInt());
            await verifyCount(1, 2);
        }
    }
}
