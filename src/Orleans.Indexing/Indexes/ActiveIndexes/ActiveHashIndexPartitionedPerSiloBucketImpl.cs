using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Concurrency;
using Orleans.Runtime;
using K = System.Object;
using V = Orleans.Indexing.IIndexableGrain;
using Orleans.Providers;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;

namespace Orleans.Indexing
{
    /// <summary>
    /// A simple implementation of a single-grain in-memory hash-index.
    /// 
    /// Generic SystemTargets are not supported yet, and that's why the
    /// implementation is non-generic.
    /// </summary>
    /// <typeparam name="K">type of hash-index key</typeparam>
    /// <typeparam name="V">type of grain that is being indexed</typeparam>
    [StorageProvider(ProviderName = Constants.MEMORY_STORAGE_PROVIDER_NAME)]
    [Reentrant]
    internal class ActiveHashIndexPartitionedPerSiloBucketImpl/*<K, V>*/ : SystemTarget, IActiveHashIndexPartitionedPerSiloBucket/*<K, V> where V : IIndexableGrain*/
    {
        private HashIndexBucketState<K, V> state;
        private readonly IndexingManager indexingManager;
        private readonly ILogger logger;
        private readonly string _parentIndexName;

        public ActiveHashIndexPartitionedPerSiloBucketImpl(IndexingManager indexingManager, string parentIndexName, GrainId grainId)
            : base(grainId, indexingManager.SiloAddress, indexingManager.LoggerFactory)
        {
            state = new HashIndexBucketState<K, V>
            {
                IndexMap = new Dictionary<K, HashIndexSingleBucketEntry<V>>(),
                IndexStatus = IndexStatus.Available
                //, IsUnique = false; //a per-silo index cannot check for uniqueness
            };

            _parentIndexName = parentIndexName;
            this.indexingManager = indexingManager;
            this.logger = indexingManager.LoggerFactory.CreateLoggerWithFullCategoryName<ActiveHashIndexPartitionedPerSiloBucketImpl>();
        }

        public async Task<bool> DirectApplyIndexUpdateBatch(Immutable<IDictionary<IIndexableGrain, IList<IMemberUpdate>>> iUpdates, bool isUnique, IndexMetaData idxMetaData, SiloAddress siloAddress = null)
        {
            logger.Trace($"ParentIndex {_parentIndexName}: Started calling DirectApplyIndexUpdateBatch with the following parameters: isUnique = {isUnique}, siloAddress = {siloAddress}, iUpdates = {MemberUpdate.UpdatesToString(iUpdates.Value)}", isUnique, siloAddress);

            IDictionary<IIndexableGrain, IList<IMemberUpdate>> updates = iUpdates.Value;
            Task[] updateTasks = new Task[updates.Count()];
            int i = 0;
            foreach (var kv in updates) // vv2:  updates.Select(kv => DirectApplyIndexUpdates(kv.Key, kv.Value, isUnique, idxMetaData, siloAddress));
            {
                updateTasks[i] = DirectApplyIndexUpdates(kv.Key, kv.Value, isUnique, idxMetaData, siloAddress);
                ++i;
            }
            await Task.WhenAll(updateTasks);

            logger.Trace($"Finished calling DirectApplyIndexUpdateBatch with the following parameters: isUnique = {isUnique}, siloAddress = {siloAddress}, iUpdates = {MemberUpdate.UpdatesToString(iUpdates.Value)}");

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private async Task DirectApplyIndexUpdates(IIndexableGrain g, IList<IMemberUpdate> updates, bool isUniqueIndex, IndexMetaData idxMetaData, SiloAddress siloAddress)
        {
            foreach (IMemberUpdate updt in updates)
            {
                await DirectApplyIndexUpdate(g, updt, isUniqueIndex, idxMetaData, siloAddress);
            }
        }

        public Task<bool> DirectApplyIndexUpdate(IIndexableGrain g, Immutable<IMemberUpdate> iUpdate, bool isUniqueIndex, IndexMetaData idxMetaData, SiloAddress siloAddress)
        {
            return DirectApplyIndexUpdate(g, iUpdate.Value, isUniqueIndex, idxMetaData, siloAddress);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Task<bool> DirectApplyIndexUpdate(IIndexableGrain g, IMemberUpdate updt, bool isUniqueIndex, IndexMetaData idxMetaData, SiloAddress siloAddress)
        {
            V updatedGrain = g;
            HashIndexBucketUtils.UpdateBucket(updatedGrain, updt, state, isUniqueIndex, idxMetaData);
            return Task.FromResult(true);
        }

        public async Task Lookup(IOrleansQueryResultStream<V> result, K key)
        {
            logger.Trace($"Streamed index lookup called for key = {key}");

            if (!(state.IndexStatus == IndexStatus.Available))
            {
                var e = new Exception(string.Format("Index is not still available."));
                logger.Error(IndexingErrorCode.IndexingIndexIsNotReadyYet_SystemTargetBucket1, $"ParentIndex {_parentIndexName}: Index is not still available.", e);
                throw e;
            }
            if (state.IndexMap.TryGetValue(key, out HashIndexSingleBucketEntry<V> entry) && !entry.IsTentative())
            {
                await result.OnNextBatchAsync(entry.Values);
                await result.OnCompletedAsync();
            }
            else
            {
                await result.OnCompletedAsync();
            }
        }

        public Task<IOrleansQueryResult<V>> Lookup(K key)
        {
            logger.Trace($"ParentIndex {_parentIndexName}: Eager index lookup called for key = {key}");

            if (!(state.IndexStatus == IndexStatus.Available))
            {
                var e = new Exception(string.Format("Index is not still available."));
                logger.Error(IndexingErrorCode.IndexingIndexIsNotReadyYet_SystemTargetBucket2, $"ParentIndex {_parentIndexName}: Index is not still available.", e);
                throw e;
            }
            var entryValues = (state.IndexMap.TryGetValue(key, out HashIndexSingleBucketEntry<V> entry) && !entry.IsTentative()) ? entry.Values : Enumerable.Empty<V>();
            return Task.FromResult((IOrleansQueryResult<V>)new OrleansQueryResult<V>(entryValues));
        }

        public Task<V> LookupUnique(K key)
        {
            if (!(state.IndexStatus == IndexStatus.Available))
            {
                var e = new Exception(string.Format("Index is not still available."));
                logger.Error(IndexingErrorCode.IndexingIndexIsNotReadyYet_SystemTargetBucket3, $"ParentIndex {_parentIndexName}: {e.Message}", e);
                throw e;
            }
            if (state.IndexMap.TryGetValue(key, out HashIndexSingleBucketEntry<V> entry) && !entry.IsTentative())
            {
                if (entry.Values.Count() == 1)
                {
                    return Task.FromResult(entry.Values.GetEnumerator().Current);
                }
                else
                {
                    var e = new Exception(string.Format("There are {0} values for the unique lookup key \"{1}\" does not exist on index \"{2}->{3}\".", entry.Values.Count(), key, _parentIndexName, IndexUtils.GetIndexNameFromIndexGrain(this)));
                    logger.Error(IndexingErrorCode.IndexingIndexIsNotReadyYet_SystemTargetBucket4, $"ParentIndex {_parentIndexName}: {e.Message}", e);
                    throw e;
                }
            }
            else
            {
                var e = new Exception(string.Format("The lookup key \"{0}\" does not exist on index \"{1}->{2}\".", key, _parentIndexName, IndexUtils.GetIndexNameFromIndexGrain(this)));
                logger.Error(IndexingErrorCode.IndexingIndexIsNotReadyYet_SystemTargetBucket5, $"ParentIndex {_parentIndexName}: {e.Message}", e);
                throw e;
            }
        }

        public Task Dispose()
        {
            state.IndexStatus = IndexStatus.Disposed;
            state.IndexMap.Clear();
            //vv2err UnregisterSystemTarget not available     this.indexingManager.Silo.UnregisterSystemTarget(this);
            return Task.CompletedTask;
        }

        public Task<bool> IsAvailable()
        {
            return Task.FromResult(state.IndexStatus == IndexStatus.Available);
        }
    }
}
