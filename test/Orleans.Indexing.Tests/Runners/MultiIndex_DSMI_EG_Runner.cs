using Orleans.Providers;
using System;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Indexing.Tests
{
    #region PartitionedPerKey

    // NFT only; FT cannot be configured to be Eager.

    [Serializable]
    public class NFT_Props_UIUSNINS_DSMI_EG_PK : ITestIndexProperties
    {
        [StorageManagedIndex(IsEager = true, IsUnique = true, NullValue = "-1")]
        public int UniqueInt { get; set; }

        [StorageManagedIndex(IsEager = true, IsUnique = true)]
        public string UniqueString { get; set; }

        [StorageManagedIndex(IsEager = true, IsUnique = false, NullValue = "0")]
        public int NonUniqueInt { get; set; }

        [StorageManagedIndex(IsEager = true, IsUnique = false)]
        public string NonUniqueString { get; set; }
    }

    [Serializable]
    public class NFT_State_UIUSNINS_DSMI_EG_PK : NFT_Props_UIUSNINS_DSMI_EG_PK, ITestIndexState
    {
        public string UnIndexedString { get; set; }
    }

    public interface INFT_Grain_UIUSNINS_DSMI_EG_PK : ITestIndexGrain, IIndexableGrain<NFT_Props_UIUSNINS_DSMI_EG_PK>
    {
    }

    [StorageProvider(ProviderName = IndexingTestConstants.CosmosDBGrainStorage)]
    public class NFT_Grain_UIUSNINS_DSMI_EG_PK : TestIndexGrainNonFaultTolerant<NFT_State_UIUSNINS_DSMI_EG_PK, NFT_Props_UIUSNINS_DSMI_EG_PK>,
                                                 INFT_Grain_UIUSNINS_DSMI_EG_PK
    {
    }
    #endregion // PartitionedPerKey

    #region PartitionedPerSilo

    // NFT only; FT cannot be configured to be Eager.

    [System.Serializable]
    public class NFT_Props_UIUSNINS_DSMI_EG_PS : ITestIndexProperties
    {
        [StorageManagedIndex(IsEager = true, IsUnique = true, NullValue = "-1")]
        public int UniqueInt { get; set; }

        [StorageManagedIndex(IsEager = true, IsUnique = true)]
        public string UniqueString { get; set; }

        [StorageManagedIndex(IsEager = true, IsUnique = false, NullValue = "0")]
        public int NonUniqueInt { get; set; }

        [StorageManagedIndex(IsEager = true, IsUnique = false)]
        public string NonUniqueString { get; set; }
    }

    [Serializable]
    public class NFT_State_UIUSNINS_DSMI_EG_PS : NFT_Props_UIUSNINS_DSMI_EG_PS, ITestIndexState
    {
        public string UnIndexedString { get; set; }
    }

    public interface INFT_Grain_UIUSNINS_DSMI_EG_PS : ITestIndexGrain, IIndexableGrain<NFT_Props_UIUSNINS_DSMI_EG_PS>
    {
    }

    [StorageProvider(ProviderName = IndexingTestConstants.CosmosDBGrainStorage)]
    public class NFT_Grain_UIUSNINS_DSMI_EG_PS : TestIndexGrainNonFaultTolerant<NFT_State_UIUSNINS_DSMI_EG_PS, NFT_Props_UIUSNINS_DSMI_EG_PS>,
                                                 INFT_Grain_UIUSNINS_DSMI_EG_PS
    {
    }
    #endregion // PartitionedPerSilo

    #region SingleBucket

    // NFT only; FT cannot be configured to be Eager.

    [System.Serializable]
    public class NFT_Props_UIUSNINS_DSMI_EG_SB : ITestIndexProperties
    {
        [StorageManagedIndex(IsEager = true, IsUnique = true, NullValue = "-1")]
        public int UniqueInt { get; set; }

        [StorageManagedIndex(IsEager = true, IsUnique = true)]
        public string UniqueString { get; set; }

        [StorageManagedIndex(IsEager = true, IsUnique = false, NullValue = "0")]
        public int NonUniqueInt { get; set; }

        [StorageManagedIndex(IsEager = true, IsUnique = false)]
        public string NonUniqueString { get; set; }
    }

    [Serializable]
    public class NFT_State_UIUSNINS_DSMI_EG_SB : NFT_Props_UIUSNINS_DSMI_EG_SB, ITestIndexState
    {
        public string UnIndexedString { get; set; }
    }

    public interface INFT_Grain_UIUSNINS_DSMI_EG_SB : ITestIndexGrain, IIndexableGrain<NFT_Props_UIUSNINS_DSMI_EG_SB>
    {
    }

    [StorageProvider(ProviderName = IndexingTestConstants.CosmosDBGrainStorage)]
    public class NFT_Grain_UIUSNINS_DSMI_EG_SB : TestIndexGrainNonFaultTolerant<NFT_State_UIUSNINS_DSMI_EG_SB, NFT_Props_UIUSNINS_DSMI_EG_SB>,
                                                 INFT_Grain_UIUSNINS_DSMI_EG_SB
    {
    }
    #endregion // SingleBucket

    public abstract class MultiIndex_DSMI_EG_Runner : IndexingTestRunnerBase
    {
        protected MultiIndex_DSMI_EG_Runner(BaseIndexingFixture fixture, ITestOutputHelper output)
            : base(fixture, output)
        {
        }

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_NFT_Grain_UIUSNINS_DSMI_EG_PK()
        {
            await base.TestIndexesWithDeactivations<INFT_Grain_UIUSNINS_DSMI_EG_PK, NFT_Props_UIUSNINS_DSMI_EG_PK>();
        }

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_NFT_Grain_UIUSNINS_DSMI_EG_PS()
        {
            await base.TestIndexesWithDeactivations<INFT_Grain_UIUSNINS_DSMI_EG_PS, NFT_Props_UIUSNINS_DSMI_EG_PS>();
        }

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_NFT_Grain_UIUSNINS_DSMI_EG_SB()
        {
            await base.TestIndexesWithDeactivations<INFT_Grain_UIUSNINS_DSMI_EG_SB, NFT_Props_UIUSNINS_DSMI_EG_SB>();
        }
    }
}
