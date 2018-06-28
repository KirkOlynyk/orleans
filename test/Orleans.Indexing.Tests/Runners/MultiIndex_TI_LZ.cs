using Orleans.Providers;
using System;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Indexing.Tests
{
    #region PartitionedPerKey
    [System.Serializable]
    public class FT_Props_UIUSNINS_TI_LZ_PK : ITestIndexProperties
    {
        [Index(typeof(TotalHashIndexPartitionedPerKey<int, IFT_Grain_UIUSNINS_TI_LZ_PK>), IsEager = false, IsUnique = true, NullValue = "0")]
        public int UniqueInt { get; set; }

        [Index(typeof(TotalHashIndexPartitionedPerKey<string, IFT_Grain_UIUSNINS_TI_LZ_PK>), IsEager = false, IsUnique = true)]
        public string UniqueString { get; set; }

        [Index(typeof(TotalHashIndexPartitionedPerKey<int, IFT_Grain_UIUSNINS_TI_LZ_PK>), IsEager = false, IsUnique = false, NullValue = "-1")]
        public int NonUniqueInt { get; set; }

        [Index(typeof(TotalHashIndexPartitionedPerKey<string, IFT_Grain_UIUSNINS_TI_LZ_PK>), IsEager = false, IsUnique = false)]
        public string NonUniqueString { get; set; }
    }

    [System.Serializable]
    public class NFT_Props_UIUSNINS_TI_LZ_PK : ITestIndexProperties
    {
        [Index(typeof(TotalHashIndexPartitionedPerKey<int, INFT_Grain_UIUSNINS_TI_LZ_PK>), IsEager = false, IsUnique = true, NullValue = "-1")]
        public int UniqueInt { get; set; }

        [Index(typeof(TotalHashIndexPartitionedPerKey<string, INFT_Grain_UIUSNINS_TI_LZ_PK>), IsEager = false, IsUnique = true)]
        public string UniqueString { get; set; }

        [Index(typeof(TotalHashIndexPartitionedPerKey<int, INFT_Grain_UIUSNINS_TI_LZ_PK>), IsEager = false, IsUnique = false, NullValue = "0")]
        public int NonUniqueInt { get; set; }

        [Index(typeof(TotalHashIndexPartitionedPerKey<string, INFT_Grain_UIUSNINS_TI_LZ_PK>), IsEager = false, IsUnique = false)]
        public string NonUniqueString { get; set; }
    }

    [Serializable]
    public class FT_State_UIUSNINS_TI_LZ_PK : FT_Props_UIUSNINS_TI_LZ_PK, ITestIndexState
    {
        public string UnIndexedString { get; set; }
    }

    [Serializable]
    public class NFT_State_UIUSNINS_TI_LZ_PK : NFT_Props_UIUSNINS_TI_LZ_PK, ITestIndexState
    {
        public string UnIndexedString { get; set; }
    }

    // TODO: Indexes are based on InterfaceType but not ClassType, so currently, unique index tests run in parallel must have 
    // distinct interfaces, which percolates to state and properties as well.
    public interface IFT_Grain_UIUSNINS_TI_LZ_PK : ITestIndexGrain, IIndexableGrain<FT_Props_UIUSNINS_TI_LZ_PK>
    {
    }

    public interface INFT_Grain_UIUSNINS_TI_LZ_PK : ITestIndexGrain, IIndexableGrain<NFT_Props_UIUSNINS_TI_LZ_PK>
    {
    }

    [StorageProvider(ProviderName = IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
    public class FT_Grain_UIUSNINS_TI_LZ_PK : TestIndexGrain<FT_State_UIUSNINS_TI_LZ_PK, FT_Props_UIUSNINS_TI_LZ_PK>,
                                                 IFT_Grain_UIUSNINS_TI_LZ_PK
    {
    }

    [StorageProvider(ProviderName = IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
    public class NFT_Grain_UIUSNINS_TI_LZ_PK : TestIndexGrainNonFaultTolerant<NFT_State_UIUSNINS_TI_LZ_PK, NFT_Props_UIUSNINS_TI_LZ_PK>,
                                                 INFT_Grain_UIUSNINS_TI_LZ_PK
    {
    }
    #endregion // PartitionedPerKey

    #region PartitionedPerSilo

    // None; Total indexes cannot be specified as partitioned per silo.

    #endregion // PartitionedPerSilo

    #region SingleBucket
    [System.Serializable]
    public class FT_Props_UIUSNINS_TI_LZ_SB : ITestIndexProperties
    {
        [Index(typeof(ITotalHashIndexSingleBucket<int, IFT_Grain_UIUSNINS_TI_LZ_SB>), IsEager = false, IsUnique = true, NullValue = "0")]
        public int UniqueInt { get; set; }

        [Index(typeof(ITotalHashIndexSingleBucket<string, IFT_Grain_UIUSNINS_TI_LZ_SB>), IsEager = false, IsUnique = true)]
        public string UniqueString { get; set; }

        [Index(typeof(ITotalHashIndexSingleBucket<int, IFT_Grain_UIUSNINS_TI_LZ_SB>), IsEager = false, IsUnique = false, NullValue = "-1")]
        public int NonUniqueInt { get; set; }

        [Index(typeof(ITotalHashIndexSingleBucket<string, IFT_Grain_UIUSNINS_TI_LZ_SB>), IsEager = false, IsUnique = false)]
        public string NonUniqueString { get; set; }
    }

    [System.Serializable]
    public class NFT_Props_UIUSNINS_TI_LZ_SB : ITestIndexProperties
    {
        [Index(typeof(ITotalHashIndexSingleBucket<int, INFT_Grain_UIUSNINS_TI_LZ_SB>), IsEager = false, IsUnique = true, NullValue = "-1")]
        public int UniqueInt { get; set; }

        [Index(typeof(ITotalHashIndexSingleBucket<string, INFT_Grain_UIUSNINS_TI_LZ_SB>), IsEager = false, IsUnique = true)]
        public string UniqueString { get; set; }

        [Index(typeof(ITotalHashIndexSingleBucket<int, INFT_Grain_UIUSNINS_TI_LZ_SB>), IsEager = false, IsUnique = false, NullValue = "0")]
        public int NonUniqueInt { get; set; }

        [Index(typeof(ITotalHashIndexSingleBucket<string, INFT_Grain_UIUSNINS_TI_LZ_SB>), IsEager = false, IsUnique = false)]
        public string NonUniqueString { get; set; }
    }

    [Serializable]
    public class FT_State_UIUSNINS_TI_LZ_SB : FT_Props_UIUSNINS_TI_LZ_SB, ITestIndexState
    {
        public string UnIndexedString { get; set; }
    }

    [Serializable]
    public class NFT_State_UIUSNINS_TI_LZ_SB : NFT_Props_UIUSNINS_TI_LZ_SB, ITestIndexState
    {
        public string UnIndexedString { get; set; }
    }

    // TODO: Indexes are based on InterfaceType but not ClassType, so currently, unique index tests run in parallel must have 
    // distinct interfaces, which percolates to state and properties as well.
    public interface IFT_Grain_UIUSNINS_TI_LZ_SB : ITestIndexGrain, IIndexableGrain<FT_Props_UIUSNINS_TI_LZ_SB>
    {
    }

    public interface INFT_Grain_UIUSNINS_TI_LZ_SB : ITestIndexGrain, IIndexableGrain<NFT_Props_UIUSNINS_TI_LZ_SB>
    {
    }

    [StorageProvider(ProviderName = IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
    public class FT_Grain_UIUSNINS_TI_LZ_SB : TestIndexGrain<FT_State_UIUSNINS_TI_LZ_SB, FT_Props_UIUSNINS_TI_LZ_SB>,
                                                 IFT_Grain_UIUSNINS_TI_LZ_SB
    {
    }

    [StorageProvider(ProviderName = IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
    public class NFT_Grain_UIUSNINS_TI_LZ_SB : TestIndexGrainNonFaultTolerant<NFT_State_UIUSNINS_TI_LZ_SB, NFT_Props_UIUSNINS_TI_LZ_SB>,
                                                 INFT_Grain_UIUSNINS_TI_LZ_SB
    {
    }
    #endregion // SingleBucket

    public abstract class MultiIndex_TI_LZ: IndexingTestRunnerBase
    {
        protected MultiIndex_TI_LZ(BaseIndexingFixture fixture, ITestOutputHelper output)
            : base(fixture, output)
        {
        }

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_FT_Grain_UIUSNINS_TI_LZ_PK()
        {
            await base.TestIndexesWithDeactivations<IFT_Grain_UIUSNINS_TI_LZ_PK, FT_Props_UIUSNINS_TI_LZ_PK>();
        }

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_NFT_Grain_UIUSNINS_TI_LZ_PK()
        {
            await base.TestIndexesWithDeactivations<INFT_Grain_UIUSNINS_TI_LZ_PK, NFT_Props_UIUSNINS_TI_LZ_PK>();
        }

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_FT_Grain_UIUSNINS_TI_LZ_SB()
        {
            await base.TestIndexesWithDeactivations<IFT_Grain_UIUSNINS_TI_LZ_SB, FT_Props_UIUSNINS_TI_LZ_SB>();
        }

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_NFT_Grain_UIUSNINS_TI_LZ_SB()
        {
            await base.TestIndexesWithDeactivations<INFT_Grain_UIUSNINS_TI_LZ_SB, NFT_Props_UIUSNINS_TI_LZ_SB>();
        }
    }
}
