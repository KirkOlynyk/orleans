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
    /// TODO: Generic SystemTargets are not supported yet, and that's why the implementation is non-generic.
    /// </summary>
    //Per comments for IActiveHashIndexPartitionedPerSiloBucket, we cannot use generics here.
    //<typeparam name="K">type of hash-index key</typeparam>
    //<typeparam name="V">type of grain that is being indexed</typeparam>
    [StorageProvider(ProviderName = IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
    [Reentrant]
    internal class ActiveHashIndexPartitionedPerSiloBucketImpl/*<K, V>*/ : SystemTarget, IActiveHashIndexPartitionedPerSiloBucket/*<K, V> where V : IIndexableGrain*/
    {
        private HashIndexBucketState<K, V> state;
        private readonly SiloIndexManager siloIndexManager;
        private readonly ILogger logger;
        private readonly string _parentIndexName;

        public ActiveHashIndexPartitionedPerSiloBucketImpl(SiloIndexManager siloIndexManager, string parentIndexName, GrainId grainId)
            : base(grainId, siloIndexManager.SiloAddress, siloIndexManager.LoggerFactory)
        {
            state = new HashIndexBucketState<K, V>
            {
                IndexMap = new Dictionary<K, HashIndexSingleBucketEntry<V>>(),
                IndexStatus = IndexStatus.Available
                //, IsUnique = false; //a per-silo index cannot check for uniqueness
            };

            _parentIndexName = parentIndexName;
            this.siloIndexManager = siloIndexManager;
            this.logger = siloIndexManager.LoggerFactory.CreateLoggerWithFullCategoryName<ActiveHashIndexPartitionedPerSiloBucketImpl>();
        }

        public async Task<bool> DirectApplyIndexUpdateBatch(Immutable<IDictionary<IIndexableGrain, IList<IMemberUpdate>>> iUpdates, bool isUnique, IndexMetaData idxMetaData, SiloAddress siloAddress = null)
        {
            logger.Trace($"ParentIndex {_parentIndexName}: Started calling DirectApplyIndexUpdateBatch with the following parameters: isUnique = {isUnique}," +
                         $" siloAddress = {siloAddress}, iUpdates = {MemberUpdate.UpdatesToString(iUpdates.Value)}", isUnique, siloAddress);

            await Task.WhenAll(iUpdates.Value.Select(kvp => DirectApplyIndexUpdates(kvp.Key, kvp.Value, isUnique, idxMetaData, siloAddress)));

            logger.Trace($"Finished calling DirectApplyIndexUpdateBatch with the following parameters: isUnique = {isUnique}, siloAddress = {siloAddress}," +
                         $" iUpdates = {MemberUpdate.UpdatesToString(iUpdates.Value)}");
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
            this.siloIndexManager.Silo.UnregisterSystemTarget(this);
            return Task.CompletedTask;
        }

        public Task<bool> IsAvailable()
        {
            return Task.FromResult(state.IndexStatus == IndexStatus.Available);
        }
    }
}
