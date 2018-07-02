﻿using Orleans.Providers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Indexing.Tests
{
    public abstract class MultiIndex_All : IndexingTestRunnerBase
    {
        protected MultiIndex_All(BaseIndexingFixture fixture, ITestOutputHelper output)
            : base(fixture, output)
        {
        }

        [Fact, TestCategory("BVT"), TestCategory("Indexing")]
        public async Task Test_MultiIndex_All()
        {
            const int NumRepsPerTest = 3;
            IEnumerable<Task> getTasks(Func<IndexingTestRunnerBase, int, Task>[] getTasksFunc)
            {
                for (int ii = 0; ii < NumRepsPerTest; ++ii)
                {
                    foreach (var task in getTasksFunc.Select(lambda => lambda(this, ii * 1000000)))
                    {
                        yield return task;
                    }
                }
            }

            await Task.WhenAll(getTasks(MultiIndex_AI_EG.GetAllTestTasks())
                    .Concat(getTasks(MultiIndex_AI_LZ.GetAllTestTasks()))
                    .Concat(getTasks(MultiIndex_TI_EG.GetAllTestTasks()))
                    .Concat(getTasks(MultiIndex_TI_LZ.GetAllTestTasks())));
        }
    }
}
