using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Indexing.Tests
{
    public static class IndexingTestUtils
    {
        public static async Task<int> CountItemsStreamingIn<TIGrain, TIProperties, TQueryProp>(this IndexingTestRunnerBase runner,
                                                                Func<IndexingTestRunnerBase, TQueryProp, Tuple<IOrleansQueryable<TIGrain, TIProperties>, Func<TIGrain, Task<TQueryProp>>>> queryTupleFunc,
                                                                TQueryProp queryValue, int delayInMilliseconds = 0)
            where TIGrain : IIndexableGrain
        {
            if (delayInMilliseconds > 0)
            {
                await Task.Delay(delayInMilliseconds);
            }
            var taskCompletionSource = new TaskCompletionSource<int>();

            var queryTuple = queryTupleFunc(runner, queryValue);
            var queryItems = queryTuple.Item1;
            var queryPropAsync = queryTuple.Item2;

            int counter = 0;
            var _ = queryItems.ObserveResults(new QueryResultStreamObserver<TIGrain>(async entry =>
            {
                counter++;
                runner.Output.WriteLine("guid = {0}, location = {1}, primary key = {2}", entry, await queryPropAsync(entry), entry.GetPrimaryKeyLong());
            }, () =>
            {
                taskCompletionSource.SetResult(counter);
                return Task.CompletedTask;
            }));

            int observedCount = await taskCompletionSource.Task;
            Assert.Equal(observedCount, (await queryItems.GetResults()).Count());
            return observedCount;
        }

        #region PlayerGrain

        private static IOrleansQueryable<TIGrain, TIProperties> QueryActivePlayerGrains<TIGrain, TIProperties>(IndexingTestRunnerBase runner)
            where TIGrain : IPlayerGrain, IIndexableGrain where TIProperties : IPlayerProperties
            => runner.IndexFactory.GetActiveGrains<TIGrain, TIProperties>();

        internal static Tuple<IOrleansQueryable<TIGrain, TIProperties>, Func<TIGrain, Task<string>>> QueryByLocation<TIGrain, TIProperties>(this IndexingTestRunnerBase runner, string queryValue)
            where TIGrain : IPlayerGrain, IIndexableGrain where TIProperties : IPlayerProperties
            => Tuple.Create<IOrleansQueryable<TIGrain, TIProperties>, Func<TIGrain, Task<string>>>(
                            from item in QueryActivePlayerGrains<TIGrain, TIProperties>(runner) where item.Location == queryValue select item,
                            entry => entry.GetLocation());

        internal static Task<int> GetLocationCount<TIGrain, TIProperties>(this IndexingTestRunnerBase runner, string location, int delayInMilliseconds = 0)
            where TIGrain : IPlayerGrain, IIndexableGrain where TIProperties : IPlayerProperties
            => runner.CountItemsStreamingIn((r, v) => r.QueryByLocation<TIGrain, TIProperties>(v), location, delayInMilliseconds);

        #endregion PlayerGrain

        #region TestIndexGrain

        private static IOrleansQueryable<TIGrain, TIProperties> QueryActiveTestIndexGrains<TIGrain, TIProperties>(IndexingTestRunnerBase runner)
            where TIGrain : ITestIndexGrain, IIndexableGrain where TIProperties : ITestIndexProperties
            => runner.IndexFactory.GetActiveGrains<TIGrain, TIProperties>();

        internal static Tuple<IOrleansQueryable<TIGrain, TIProperties>, Func<TIGrain, Task<int>>> QueryByUniqueInt<TIGrain, TIProperties>(this IndexingTestRunnerBase runner, int queryValue)
            where TIGrain : ITestIndexGrain, IIndexableGrain where TIProperties : ITestIndexProperties
            => Tuple.Create<IOrleansQueryable<TIGrain, TIProperties>, Func<TIGrain, Task<int>>>(
                            from item in QueryActiveTestIndexGrains<TIGrain, TIProperties>(runner) where item.UniqueInt == queryValue select item,
                            entry => entry.GetUniqueInt());

        internal static Tuple<IOrleansQueryable<TIGrain, TIProperties>, Func<TIGrain, Task<string>>> QueryByUniqueString<TIGrain, TIProperties>(this IndexingTestRunnerBase runner, string queryValue)
            where TIGrain : ITestIndexGrain, IIndexableGrain where TIProperties : ITestIndexProperties
            => Tuple.Create<IOrleansQueryable<TIGrain, TIProperties>, Func<TIGrain, Task<string>>>(
                            from item in QueryActiveTestIndexGrains<TIGrain, TIProperties>(runner) where item.UniqueString == queryValue select item,
                            entry => entry.GetUniqueString());

        internal static Tuple<IOrleansQueryable<TIGrain, TIProperties>, Func<TIGrain, Task<int>>> QueryByNonUniqueInt<TIGrain, TIProperties>(this IndexingTestRunnerBase runner, int queryValue)
            where TIGrain : ITestIndexGrain, IIndexableGrain where TIProperties : ITestIndexProperties
            => Tuple.Create<IOrleansQueryable<TIGrain, TIProperties>, Func<TIGrain, Task<int>>>(
                            from item in QueryActiveTestIndexGrains<TIGrain, TIProperties>(runner) where item.NonUniqueInt == queryValue select item,
                            entry => entry.GetNonUniqueInt());

        internal static Tuple<IOrleansQueryable<TIGrain, TIProperties>, Func<TIGrain, Task<string>>> QueryByNonUniqueString<TIGrain, TIProperties>(this IndexingTestRunnerBase runner, string queryValue)
            where TIGrain : ITestIndexGrain, IIndexableGrain where TIProperties : ITestIndexProperties
            => Tuple.Create<IOrleansQueryable<TIGrain, TIProperties>, Func<TIGrain, Task<string>>>(
                            from item in QueryActiveTestIndexGrains<TIGrain, TIProperties>(runner) where item.NonUniqueString == queryValue select item,
                            entry => entry.GetNonUniqueString());

        internal static Task<int> GetUniqueIntCount<TIGrain, TIProperties>(this IndexingTestRunnerBase runner, int uniqueValue, int delayInMilliseconds = 0)
            where TIGrain : ITestIndexGrain, IIndexableGrain where TIProperties : ITestIndexProperties
            => runner.CountItemsStreamingIn((r, v) => r.QueryByUniqueInt<TIGrain, TIProperties>(v), uniqueValue, delayInMilliseconds);

        internal static Task<int> GetUniqueStringCount<TIGrain, TIProperties>(this IndexingTestRunnerBase runner, string uniqueValue, int delayInMilliseconds = 0)
            where TIGrain : ITestIndexGrain, IIndexableGrain where TIProperties : ITestIndexProperties
            => runner.CountItemsStreamingIn((r, v) => r.QueryByUniqueString<TIGrain, TIProperties>(v), uniqueValue, delayInMilliseconds);

        internal static Task<int> GetNonUniqueIntCount<TIGrain, TIProperties>(this IndexingTestRunnerBase runner, int nonUniqueValue, int delayInMilliseconds = 0)
            where TIGrain : ITestIndexGrain, IIndexableGrain where TIProperties : ITestIndexProperties
            => runner.CountItemsStreamingIn((r, v) => r.QueryByNonUniqueInt<TIGrain, TIProperties>(v), nonUniqueValue, delayInMilliseconds);

        internal static Task<int> GetNonUniqueStringCount<TIGrain, TIProperties>(this IndexingTestRunnerBase runner, string nonUniqueValue, int delayInMilliseconds = 0)
            where TIGrain : ITestIndexGrain, IIndexableGrain where TIProperties : ITestIndexProperties
            => runner.CountItemsStreamingIn((r, v) => r.QueryByNonUniqueString<TIGrain, TIProperties>(v), nonUniqueValue, delayInMilliseconds);

        #endregion TestIndexGrain
    }
}
