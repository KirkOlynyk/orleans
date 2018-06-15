using Orleans.Providers;
using System;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Indexing.Tests
{
    using ITC = IndexingTestConstants;

    [System.Serializable]
    public class FT_Props_UIUSNINS_AI_UQ_LZ_PK : ITestIndexProperties
    {
        [Index(typeof(ActiveHashIndexPartitionedPerKey<int, IFT_Grain_UIUSNINS_AI_UQ_LZ_PK>), IsEager = false, IsUnique = true, NullValue = "0")]
        public int UniqueInt { get; set; }

        [Index(typeof(ActiveHashIndexPartitionedPerKey<string, IFT_Grain_UIUSNINS_AI_UQ_LZ_PK>), IsEager = false, IsUnique = true)]
        public string UniqueString { get; set; }

        [Index(typeof(ActiveHashIndexPartitionedPerKey<int, IFT_Grain_UIUSNINS_AI_UQ_LZ_PK>), IsEager = false, IsUnique = false, NullValue = "-1")]
        public int NonUniqueInt { get; set; }

        [Index(typeof(ActiveHashIndexPartitionedPerKey<string, IFT_Grain_UIUSNINS_AI_UQ_LZ_PK>), IsEager = false, IsUnique = false)]
        public string NonUniqueString { get; set; }
    }

    [System.Serializable]
    public class NFT_Props_UIUSNINS_AI_UQ_LZ_PK : ITestIndexProperties
    {
        [Index(typeof(ActiveHashIndexPartitionedPerKey<int, INFT_Grain_UIUSNINS_AI_UQ_LZ_PK>), IsEager = false, IsUnique = true, NullValue = "-1")]
        public int UniqueInt { get; set; }

        [Index(typeof(ActiveHashIndexPartitionedPerKey<string, INFT_Grain_UIUSNINS_AI_UQ_LZ_PK>), IsEager = false, IsUnique = true)]
        public string UniqueString { get; set; }

        [Index(typeof(ActiveHashIndexPartitionedPerKey<int, INFT_Grain_UIUSNINS_AI_UQ_LZ_PK>), IsEager = false, IsUnique = false, NullValue = "0")]
        public int NonUniqueInt { get; set; }

        [Index(typeof(ActiveHashIndexPartitionedPerKey<string, INFT_Grain_UIUSNINS_AI_UQ_LZ_PK>), IsEager = false, IsUnique = false)]
        public string NonUniqueString { get; set; }
    }

    [Serializable]
    public class FT_State_UIUSNINS_AI_UQ_LZ_PK : FT_Props_UIUSNINS_AI_UQ_LZ_PK, ITestIndexState
    {
        public string UnIndexedString { get; set; }
    }

    [Serializable]
    public class NFT_State_UIUSNINS_AI_UQ_LZ_PK : NFT_Props_UIUSNINS_AI_UQ_LZ_PK, ITestIndexState
    {
        public string UnIndexedString { get; set; }
    }

    // TODO: Indexes are based on InterfaceType but not ClassType, so currently, unique index tests run in parallel must have 
    // distinct interfaces, which percolates to state and properties as well.
    public interface IFT_Grain_UIUSNINS_AI_UQ_LZ_PK : ITestIndexGrain, IIndexableGrain<FT_Props_UIUSNINS_AI_UQ_LZ_PK>
    {
    }

    public interface INFT_Grain_UIUSNINS_AI_UQ_LZ_PK : ITestIndexGrain, IIndexableGrain<NFT_Props_UIUSNINS_AI_UQ_LZ_PK>
    {
    }

    [StorageProvider(ProviderName = IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
    public class FT_Grain_UIUSNINS_AI_UQ_LZ_PK : TestIndexGrain<FT_State_UIUSNINS_AI_UQ_LZ_PK, FT_Props_UIUSNINS_AI_UQ_LZ_PK>,
                                                 IFT_Grain_UIUSNINS_AI_UQ_LZ_PK
    {
    }

    [StorageProvider(ProviderName = IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
    public class NFT_Grain_UIUSNINS_AI_UQ_LZ_PK : TestIndexGrainNonFaultTolerant<NFT_State_UIUSNINS_AI_UQ_LZ_PK, NFT_Props_UIUSNINS_AI_UQ_LZ_PK>,
                                                 INFT_Grain_UIUSNINS_AI_UQ_LZ_PK
    {
    }

    public abstract class MultipleUniqueAndNonUniqueRunner: IndexingTestRunnerBase
    {
        protected MultipleUniqueAndNonUniqueRunner(BaseIndexingFixture fixture, ITestOutputHelper output)
            : base(fixture, output)
        {
        }

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_FT_Grain_UIUSNINS_AI_UQ_LZ_PK_DeactivateOne()
        {
            using (var tw = new TestConsoleOutputWriter(base.Output, "start test"))
            {
                Task<IFT_Grain_UIUSNINS_AI_UQ_LZ_PK> makeGrain(int uInt, string uString, int nuInt, string nuString)
                    => this.CreateGrain<IFT_Grain_UIUSNINS_AI_UQ_LZ_PK>(uInt, uString, nuInt, nuString);
                var p1 = await makeGrain(1, "one", 1000, "1k");
                var p11 = await makeGrain(11, "eleven", 1000, "1k");
                var p2 = await makeGrain(2, "two", 2000, "2k");
                var p3 = await makeGrain(3, "three", 3000, "3k");

                var intIdexes = base.GetAndWaitForIndexes<int, IFT_Grain_UIUSNINS_AI_UQ_LZ_PK>(ITC.UniqueIntIndex, ITC.NonUniqueIntIndex);
                var stringIndexes = base.GetAndWaitForIndexes<string, IFT_Grain_UIUSNINS_AI_UQ_LZ_PK>(ITC.UniqueStringIndex, ITC.NonUniqueStringIndex);

                async Task verifyCount(int expected1, int expected1000)
                {
                    Assert.Equal(expected1, await this.GetUniqueIntCount<IFT_Grain_UIUSNINS_AI_UQ_LZ_PK, FT_Props_UIUSNINS_AI_UQ_LZ_PK>(1));
                    Assert.Equal(expected1000, await this.GetNonUniqueIntCount<IFT_Grain_UIUSNINS_AI_UQ_LZ_PK, FT_Props_UIUSNINS_AI_UQ_LZ_PK>(1000));
                }

                Console.WriteLine("*** First Verify ***");
                await verifyCount(1, 2);

                Console.WriteLine("*** Deactivate ***");
                await p11.Deactivate(ITC.DelayUntilIndexesAreUpdatedLazily);

                Console.WriteLine("*** Second Verify ***");
                await verifyCount(1, 1);

                Console.WriteLine("*** GetGrain ***");
                p11 = base.GetGrain<IFT_Grain_UIUSNINS_AI_UQ_LZ_PK>(p11.GetPrimaryKeyLong());
                Assert.Equal(1000, await p11.GetNonUniqueInt());
                Console.WriteLine("*** Third Verify ***");
                await verifyCount(1, 2);
            }
        }

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_NFT_Grain_UIUSNINS_AI_UQ_LZ_PK()
        {
            Task<INFT_Grain_UIUSNINS_AI_UQ_LZ_PK> makeGrain(int uInt, string uString, int nuInt, string nuString)
                => this.CreateGrain<INFT_Grain_UIUSNINS_AI_UQ_LZ_PK>(uInt, uString, nuInt, nuString);
            var p1 = await makeGrain(1, "one", 1000, "1k");
            var p11 = await makeGrain(11, "eleven", 1000, "1k");
            var p2 = await makeGrain(2, "two", 2000, "2k");
            var p3 = await makeGrain(3, "three", 3000, "3k");

            var intIdexes = base.GetAndWaitForIndexes<int, INFT_Grain_UIUSNINS_AI_UQ_LZ_PK>(ITC.UniqueIntIndex, ITC.NonUniqueIntIndex);
            var stringIndexes = base.GetAndWaitForIndexes<string, INFT_Grain_UIUSNINS_AI_UQ_LZ_PK>(ITC.UniqueStringIndex, ITC.NonUniqueStringIndex);

            async Task verifyCount(int expected1, int expected1000)
            {
                Assert.Equal(expected1, await this.GetUniqueIntCount<INFT_Grain_UIUSNINS_AI_UQ_LZ_PK, NFT_Props_UIUSNINS_AI_UQ_LZ_PK>(1));
                Assert.Equal(expected1000, await this.GetNonUniqueIntCount<INFT_Grain_UIUSNINS_AI_UQ_LZ_PK, NFT_Props_UIUSNINS_AI_UQ_LZ_PK>(1000));
            }

            await verifyCount(1, 2);

            await p11.Deactivate(ITC.DelayUntilIndexesAreUpdatedLazily);
            await verifyCount(1, 1);

            p11 = base.GetGrain<INFT_Grain_UIUSNINS_AI_UQ_LZ_PK>(p11.GetPrimaryKeyLong());
            Assert.Equal(1000, await p11.GetNonUniqueInt());
            await verifyCount(1, 2);
        }
    }
}
