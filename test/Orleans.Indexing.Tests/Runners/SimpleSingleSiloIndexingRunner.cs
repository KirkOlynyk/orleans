using System.Threading.Tasks;
using Xunit;
using Orleans;
using Orleans.Indexing;
using Xunit.Abstractions;
using System.Threading;
using TestExtensions;

namespace Orleans.Indexing.Tests
{
    public abstract class SimpleSingleSiloIndexingRunner : IndexingTestRunnerBase
    {
        protected SimpleSingleSiloIndexingRunner(IGrainFactory grainFactory, IClusterClient clusterClient, ITestOutputHelper output)
            : base(grainFactory, clusterClient, output)
        {
        }

        //[Fact, TestCategory("BVT"), TestCategory("Indexing")]
        //public async Task Test_Indexing_AddOneIndex()
        //{
        //    bool isLocIndexCreated = await GrainClient.GrainFactory.CreateAndRegisterIndex<HashIndexSingleBucketInterface<string, IPlayerGrain>, PlayerLocIndexGen>("locIdx1");
        //    Assert.IsTrue(isLocIndexCreated);
        //}

//[Fact, TestCategory("BVT"), TestCategory("Indexing")]
//public async Task Test_Indexing_AddTwoIndexes()
//{
//    bool isLocIndexCreated = await GrainClient.GrainFactory.CreateAndRegisterIndex<HashIndexSingleBucketInterface<string, IPlayerGrain>, PlayerLocIndexGen>("locIdx2");
//    bool isScoreIndexCreated = await GrainClient.GrainFactory.CreateAndRegisterIndex<HashIndexSingleBucketInterface<int, IPlayerGrain>, PlayerScoreIndexGen>("scoreIdx2");

//    Assert.IsTrue(isLocIndexCreated);
//    Assert.IsTrue(isScoreIndexCreated);
//}

        /// <summary>
        /// Tests basic functionality of HashIndexSingleBucket
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexLookup1()
        {
            //await clusterClient.DropAllIndexes<IPlayerGrain>();

            var p1 = base.GetGrain<IPlayer1GrainNonFaultTolerant>(1);
            await p1.SetLocation("Seattle");

            //bool isLocIndexCreated = await clusterClient.CreateAndRegisterIndex<HashIndexSingleBucketInterface<string, IPlayerGrain>, PlayerLocIndexGen>("__GetLocation");
            //Assert.IsTrue(isLocIndexCreated);

            var p2 = base.GetGrain<IPlayer1GrainNonFaultTolerant>(2);
            var p3 = base.GetGrain<IPlayer1GrainNonFaultTolerant>(3);

            await p2.SetLocation("Seattle");
            await p3.SetLocation("San Fransisco");

            IIndexInterface<string, IPlayer1GrainNonFaultTolerant> locIdx = base.GetIndex<string, IPlayer1GrainNonFaultTolerant>("__Location");

            while (!await locIdx.IsAvailable()) Thread.Sleep(50);

            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayer1GrainNonFaultTolerant, Player1PropertiesNonFaultTolerant>("Seattle"));

            await p2.Deactivate();

            Thread.Sleep(1000);

            Assert.Equal(1, await this.CountPlayersStreamingIn<IPlayer1GrainNonFaultTolerant, Player1PropertiesNonFaultTolerant>("Seattle"));

            p2 = base.GetGrain<IPlayer1GrainNonFaultTolerant>(2);
            Assert.Equal("Seattle", await p2.GetLocation());

            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayer1GrainNonFaultTolerant, Player1PropertiesNonFaultTolerant>("Seattle"));
        }

        /// <summary>
        /// Tests basic functionality of ActiveHashIndexPartitionedPerSiloImpl with 1 Silo
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexLookup2()
        {
            //await clusterClient.DropAllIndexes<IPlayerGrain>();

            IPlayer2GrainNonFaultTolerant p1 = base.GetGrain<IPlayer2GrainNonFaultTolerant>(1);
            await p1.SetLocation("Tehran");

            //bool isLocIndexCreated = await clusterClient.CreateAndRegisterIndex<HashIndexSingleBucketInterface<string, IPlayerGrain>, PlayerLocIndexGen>("__GetLocation");
            //Assert.IsTrue(isLocIndexCreated);

            IPlayer2GrainNonFaultTolerant p2 = base.GetGrain<IPlayer2GrainNonFaultTolerant>(2);
            IPlayer2GrainNonFaultTolerant p3 = base.GetGrain<IPlayer2GrainNonFaultTolerant>(3);

            await p2.SetLocation("Tehran");
            await p3.SetLocation("Yazd");

            IIndexInterface<string, IPlayer2GrainNonFaultTolerant> locIdx = base.GetIndex<string, IPlayer2GrainNonFaultTolerant>("__Location");

            while (!await locIdx.IsAvailable()) Thread.Sleep(50);

            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayer2GrainNonFaultTolerant, Player2PropertiesNonFaultTolerant>("Tehran"));

            await p2.Deactivate();

            Thread.Sleep(1000);

            Assert.Equal(1, await this.CountPlayersStreamingIn<IPlayer2GrainNonFaultTolerant, Player2PropertiesNonFaultTolerant>("Tehran"));

            p2 = base.GetGrain<IPlayer2GrainNonFaultTolerant>(2);
            Assert.Equal("Tehran", await p2.GetLocation());

            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayer2GrainNonFaultTolerant, Player2PropertiesNonFaultTolerant>("Tehran"));
        }

        /// <summary>
        /// Tests basic functionality of HashIndexPartitionedPerKey
        /// </summary>
        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_Indexing_IndexLookup4()
        {
            //await clusterClient.DropAllIndexes<IPlayerGrain>();

            IPlayer3GrainNonFaultTolerant p1 = base.GetGrain<IPlayer3GrainNonFaultTolerant>(1);
            await p1.SetLocation("Seattle");

            //bool isLocIndexCreated = await clusterClient.CreateAndRegisterIndex<HashIndexSingleBucketInterface<string, IPlayerGrain>, PlayerLocIndexGen>("__Location");
            //Assert.IsTrue(isLocIndexCreated);

            IPlayer3GrainNonFaultTolerant p2 = base.GetGrain<IPlayer3GrainNonFaultTolerant>(2);
            IPlayer3GrainNonFaultTolerant p3 = base.GetGrain<IPlayer3GrainNonFaultTolerant>(3);

            await p2.SetLocation("Seattle");
            await p3.SetLocation("San Fransisco");

            IIndexInterface<string, IPlayer3GrainNonFaultTolerant> locIdx = base.GetIndex<string, IPlayer3GrainNonFaultTolerant>("__Location");

            while (!await locIdx.IsAvailable()) Thread.Sleep(50);

            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayer3GrainNonFaultTolerant, Player3PropertiesNonFaultTolerant>("Seattle"));

            await p2.Deactivate();

            Thread.Sleep(1000);

            Assert.Equal(1, await this.CountPlayersStreamingIn<IPlayer3GrainNonFaultTolerant, Player3PropertiesNonFaultTolerant>("Seattle"));

            p2 = base.GetGrain<IPlayer3GrainNonFaultTolerant>(2);
            Assert.Equal("Seattle", await p2.GetLocation());

            Assert.Equal(2, await this.CountPlayersStreamingIn<IPlayer3GrainNonFaultTolerant, Player3PropertiesNonFaultTolerant>("Seattle"));
        }
    }
}
