using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using UnitTests.GrainInterfaces;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Indexing.Tests
{
    public abstract class ActiveGrainEnumerationRunner : IndexingTestRunnerBase
    {
        protected ActiveGrainEnumerationRunner(BaseIndexingFixture fixture, ITestOutputHelper output)
            : base(fixture, output)
        {
        }

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task FindActiveGrains()
        {
            await base.StartAndWaitForSecondSilo();

            // create grains
            base.Output.WriteLine("creating and activating grains");
            var grain1 = base.GetGrain<ISimpleGrain>(1);
            var grain2 = base.GetGrain<ISimpleGrain>(2);
            var grain3 = base.GetGrain<ISimpleGrain>(3);
            await grain1.GetA();
            await grain2.GetA();
            await grain3.GetA();

            //enumerate active grains
            base.Output.WriteLine("\n\nour own grain statistics");
            IActiveGrainEnumeratorGrain enumGrain = base.GetGrain<IActiveGrainEnumeratorGrain>(0);
            IEnumerable<Guid> activeGrains = enumGrain.GetActiveGrains("UnitTests.Grains.SimpleGrain").Result;
            foreach (var entry in activeGrains)
            {
                base.Output.WriteLine("guid = {0}", entry);
            }

            Assert.Equal(3, activeGrains.AsQueryable().Count());
        }

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task FindActiveGrains_typed()
        {
            await base.StartAndWaitForSecondSilo();

            // create grains
            base.Output.WriteLine("creating and activating grains");
            var grain1 = base.GetGrain<ISimpleGrain>(1);
            var grain2 = base.GetGrain<ISimpleGrain>(2);
            var grain3 = base.GetGrain<ISimpleGrain>(3);
            await grain1.GetA();
            await grain2.GetA();
            await grain3.GetA();

            //enumerate active grains
            base.Output.WriteLine("\n\nour own grain statistics");
            IActiveGrainEnumeratorGrain enumGrain = base.GetGrain<IActiveGrainEnumeratorGrain>(0);
            IEnumerable<IGrain> activeGrains = enumGrain.GetActiveGrains(typeof(ISimpleGrain)).Result;
            foreach (var entry in activeGrains)
            {
                base.Output.WriteLine("guid = {0}", entry.GetPrimaryKey());
            }

            Assert.Equal(3, activeGrains.AsQueryable().Count());
        }
    }
}
