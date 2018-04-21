using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Concurrency;
using Orleans.Runtime;

namespace Orleans.Indexing
{
    /// <summary>
    /// A simple implementation of a partitioned in-memory hash-index
    /// </summary>
    /// <typeparam name="K">type of hash-index key</typeparam>
    /// <typeparam name="V">type of grain that is being indexed</typeparam>
    /// <typeparam name="BucketT">type of bucket for the index</typeparam>
    public abstract class HashIndexPartitionedPerKey<K, V, BucketT> : IHashIndexInterface<K, V> where V : class, IIndexableGrain
        where BucketT : IHashIndexPartitionedPerKeyBucketInterface<K, V>, IGrainWithStringKey
    {
        private string _indexName;
        //private bool _isUnique;   // TODO uniqueness currently not allowed

        private readonly IndexManager indexManager;
        private readonly ILogger logger;

        public HashIndexPartitionedPerKey(IServiceProvider serviceProvider, string indexName, bool isUniqueIndex)
        {
            this._indexName = indexName;
            //_isUnique = isUniqueIndex;
            this.indexManager = IndexManager.GetIndexManager(serviceProvider);
            this.logger = this.indexManager.LoggerFactory.CreateLoggerWithFullCategoryName<HashIndexPartitionedPerKey<K, V, BucketT>>();
        }

        public async Task<bool> DirectApplyIndexUpdateBatch(Immutable<IDictionary<IIndexableGrain, IList<IMemberUpdate>>> iUpdates, bool isUnique, IndexMetaData idxMetaData, SiloAddress siloAddress = null)
        {
            logger.Trace($"Started calling DirectApplyIndexUpdateBatch with the following parameters: isUnique = {isUnique}, siloAddress = {siloAddress}," +
                         $" iUpdates = {MemberUpdate.UpdatesToString(iUpdates.Value)}");

            IDictionary<IIndexableGrain, IList<IMemberUpdate>> updates = iUpdates.Value;
            IDictionary<int, IDictionary<IIndexableGrain, IList<IMemberUpdate>>> bucketUpdates = new Dictionary<int, IDictionary<IIndexableGrain, IList<IMemberUpdate>>>();
            foreach (var kv in updates)
            {
                IIndexableGrain g = kv.Key;
                IList<IMemberUpdate> gUpdates = kv.Value;
                foreach (IMemberUpdate update in gUpdates)
                {
                    IndexOperationType opType = update.GetOperationType();
                    if (opType == IndexOperationType.Update)
                    {
                        int befImgHash = update.GetBeforeImage().GetHashCode();
                        int aftImgHash = update.GetAfterImage().GetHashCode();

                        if (befImgHash == aftImgHash)
                        {
                            AddUpdateToBucket(bucketUpdates, g, befImgHash, update);
                        }
                        else
                        {
                            AddUpdateToBucket(bucketUpdates, g, befImgHash, new MemberUpdateOverridenOperation(update, IndexOperationType.Delete));
                            AddUpdateToBucket(bucketUpdates, g, aftImgHash, new MemberUpdateOverridenOperation(update, IndexOperationType.Insert));
                        }
                    }
                    else if (opType == IndexOperationType.Insert)
                    {
                        int aftImgHash = update.GetAfterImage().GetHashCode();
                        AddUpdateToBucket(bucketUpdates, g, aftImgHash, update);
                    }
                    else if (opType == IndexOperationType.Delete)
                    {
                        int befImgHash = update.GetBeforeImage().GetHashCode();
                        AddUpdateToBucket(bucketUpdates, g, befImgHash, update);
                    }
                }
            }

            var results = await Task.WhenAll(bucketUpdates.Select(kv =>
            {
                BucketT bucket = GetGrain(IndexUtils.GetIndexGrainID(typeof(V), this._indexName) + "_" + kv.Key);
                return bucket.DirectApplyIndexUpdateBatch(kv.Value.AsImmutable(), isUnique, idxMetaData, siloAddress);
            }));

            logger.Trace($"Finished calling DirectApplyIndexUpdateBatch with the following parameters: isUnique = {isUnique}, siloAddress = {siloAddress}," +
                         $" iUpdates = {MemberUpdate.UpdatesToString(iUpdates.Value)}, results = '{string.Join(", ", results)}'");

            return true;
        }

        private BucketT GetGrain(string key) => this.indexManager.GrainFactory.GetGrain<BucketT>(key);

        /// <summary>
        /// Adds an grain update to the bucketUpdates dictionary
        /// </summary>
        /// <param name="bucketUpdates">the bucketUpdates dictionary</param>
        /// <param name="g">target grain</param>
        /// <param name="bucket">the bucket index</param>
        /// <param name="update">the update to be added</param>
        private void AddUpdateToBucket(IDictionary<int, IDictionary<IIndexableGrain, IList<IMemberUpdate>>> bucketUpdates, IIndexableGrain g, int bucket, IMemberUpdate update)
        {
            if (bucketUpdates.TryGetValue(bucket, out IDictionary<IIndexableGrain, IList<IMemberUpdate>> tmpBucketUpdatesMap))
            {
                if (!tmpBucketUpdatesMap.TryGetValue(g, out IList<IMemberUpdate> tmpUpdateList))
                {
                    tmpUpdateList = new List<IMemberUpdate>(new[] { update });
                    tmpBucketUpdatesMap.Add(g, tmpUpdateList);
                }
                else
                {
                    tmpUpdateList.Add(update);
                }
            }
            else
            {
                tmpBucketUpdatesMap = new Dictionary<IIndexableGrain, IList<IMemberUpdate>>
                {
                    { g, new List<IMemberUpdate>(new[] { update }) }
                };
                bucketUpdates.Add(bucket, tmpBucketUpdatesMap);
            }
        }

        public async Task<bool> DirectApplyIndexUpdate(IIndexableGrain g, Immutable<IMemberUpdate> iUpdate, bool isUniqueIndex, IndexMetaData idxMetaData, SiloAddress siloAddress)
        {
            IMemberUpdate update = iUpdate.Value;
            IndexOperationType opType = update.GetOperationType();
            if (opType == IndexOperationType.Update)
            {
                int befImgHash = update.GetBeforeImage().GetHashCode();
                int aftImgHash = update.GetAfterImage().GetHashCode();
                BucketT befImgBucket = GetGrain(IndexUtils.GetIndexGrainID(typeof(V), this._indexName) + "_" + befImgHash);
                if (befImgHash == aftImgHash)
                {
                    return await befImgBucket.DirectApplyIndexUpdate(g, iUpdate, isUniqueIndex, idxMetaData);
                }

                BucketT aftImgBucket = GetGrain(IndexUtils.GetIndexGrainID(typeof(V), this._indexName) + "_" + befImgHash);
                var befTask = befImgBucket.DirectApplyIndexUpdate(g, new MemberUpdateOverridenOperation(iUpdate.Value, IndexOperationType.Delete).AsImmutable<IMemberUpdate>(), isUniqueIndex, idxMetaData);
                var aftTask = aftImgBucket.DirectApplyIndexUpdate(g, new MemberUpdateOverridenOperation(iUpdate.Value, IndexOperationType.Insert).AsImmutable<IMemberUpdate>(), isUniqueIndex, idxMetaData);
                bool[] results = await Task.WhenAll(befTask, aftTask);
                return results[0] && results[1];
            }
            else if (opType == IndexOperationType.Insert)
            {
                int aftImgHash = update.GetAfterImage().GetHashCode();
                BucketT aftImgBucket = GetGrain(IndexUtils.GetIndexGrainID(typeof(V), this._indexName) + "_" + aftImgHash);
                return await aftImgBucket.DirectApplyIndexUpdate(g, iUpdate, isUniqueIndex, idxMetaData);
            }
            else if (opType == IndexOperationType.Delete)
            {
                int befImgHash = update.GetBeforeImage().GetHashCode();
                BucketT befImgBucket = GetGrain(IndexUtils.GetIndexGrainID(typeof(V), this._indexName) + "_" + befImgHash);
                return await befImgBucket.DirectApplyIndexUpdate(g, iUpdate, isUniqueIndex, idxMetaData);
            }
            return true;
        }

        public Task Lookup(IOrleansQueryResultStream<V> result, K key)
        {
            logger.Trace($"Streamed index lookup called for key = {key}");

            BucketT targetBucket = GetGrain(IndexUtils.GetIndexGrainID(typeof(V), this._indexName) + "_" + key.GetHashCode());
            return targetBucket.Lookup(result, key);
        }

        public async Task<V> LookupUnique(K key)
        {
            var result = new OrleansFirstQueryResultStream<V>();
            var taskCompletionSource = new TaskCompletionSource<V>();
            Task<V> tsk = taskCompletionSource.Task;
            Action<V> responseHandler = taskCompletionSource.SetResult;
            await result.SubscribeAsync(new QueryFirstResultStreamObserver<V>(responseHandler));
            await Lookup(result, key);
            return await tsk;
        }

        public Task Dispose()
        {
            // TODO Right now we cannot do anything; we need to know the list of buckets.
            return Task.CompletedTask;
        }

        public Task<bool> IsAvailable() => Task.FromResult(true);

        Task IIndexInterface.Lookup(IOrleansQueryResultStream<IIndexableGrain> result, object key) => Lookup(result.Cast<V>(), (K)key);

        public Task<IOrleansQueryResult<V>> Lookup(K key)
        {
            logger.Trace($"Eager index lookup called for key = {key}");

            BucketT targetBucket = GetGrain(IndexUtils.GetIndexGrainID(typeof(V), this._indexName) + "_" + key.GetHashCode());
            return targetBucket.Lookup(key);
        }

        async Task<IOrleansQueryResult<IIndexableGrain>> IIndexInterface.Lookup(object key)
            => await Lookup((K)key);
    }
}
