using System;
using System.Linq;
using System.Threading.Tasks;
using Orleans;
using Orleans.Indexing;
using UnitTests.GrainInterfaces;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Indexing.Tests
{
    public static class IndexingTestUtils
    {
        public static async Task<int> CountPlayersStreamingIn<TIGrain, TProperties>(this IndexingTestRunnerBase runner, string city, int delayInMiliseconds = 0)
            where TIGrain : IPlayerGrain, IIndexableGrain
            where TProperties : IPlayerProperties
        {
            if(delayInMiliseconds > 0)
            {
                //wait for one second
                await Task.Delay(delayInMiliseconds);
            }
            var taskCompletionSource = new TaskCompletionSource<int>();
            Task<int> tsk = taskCompletionSource.Task;
            Action<int> responseHandler = taskCompletionSource.SetResult;

            IOrleansQueryable<TIGrain, TProperties> q = from player in runner.GrainFactory.GetActiveGrains<TIGrain, TProperties>(runner.IndexingStreamProvider)
                                                        where player.Location == city
                                                        select player;
            
            int counter = 0;
            var _ = q.ObserveResults(new QueryResultStreamObserver<TIGrain>(async entry =>
            {
                counter++;
                runner.Output.WriteLine("guid = {0}, location = {1}, primary key = {2}", entry, await entry.GetLocation(), entry.GetPrimaryKeyLong());
            }, () => {
                responseHandler(counter);
                return Task.CompletedTask;
            }));

            int finalCount = await tsk;

            Assert.Equal(finalCount, await CountPlayersBlockingIn(q));

            return finalCount;
        }

        private static async Task<int> CountPlayersBlockingIn<TIGrain, TProperties>(IOrleansQueryable<TIGrain, TProperties> q)
            where TIGrain : IPlayerGrain, IIndexableGrain
            where TProperties : IPlayerProperties
        {
            IOrleansQueryResult<TIGrain> result = await q.GetResults();
            return result.Count();
        }
    }
}
