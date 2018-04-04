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
    /// A simple implementation of a single-grain in-memory hash-index
    /// </summary>
    /// <typeparam name="K">type of hash-index key</typeparam>
    /// <typeparam name="V">type of grain that is being indexed</typeparam>
    [Reentrant]
    //[StatelessWorker]
    //TODO: because of a bug in OrleansStreams, this grain cannot be StatelessWorker. It should be fixed later. vv2 which bug?
    //TODO: basically, this class does not even need to be a grain, but it's not possible to call a SystemTarget from a non-grain
    public class ActiveHashIndexPartitionedPerSiloImpl<K, V> : Grain, IActiveHashIndexPartitionedPerSilo<K, V> where V : class, IIndexableGrain
    {
        private IndexStatus _status;
        private readonly IndexManager indexManager;
        private readonly ILogger logger;

        public ActiveHashIndexPartitionedPerSiloImpl()
        {
            this.indexManager = IndexManager.GetIndexManager(base.ServiceProvider);
            this.logger = this.indexManager.LoggerFactory.CreateLoggerWithFullCategoryName<ActiveHashIndexPartitionedPerSiloImpl<K, V>>();
        }

        internal static void InitPerSilo(IndexManager indexManager, string indexName, bool isUnique)
        {
            indexManager.Silo.RegisterSystemTarget(new ActiveHashIndexPartitionedPerSiloBucketImpl(indexManager, indexName, GetGrainID(indexName)));
        }

        public override Task OnActivateAsync()
        {
            _status = IndexStatus.Available;
            return base.OnActivateAsync();
        }

        /// <summary>
        /// DirectApplyIndexUpdateBatch is not supported on ActiveHashIndexPartitionedPerSiloImpl,
        /// because it will be skipped via IndexExtensions.DirectApplyIndexUpdateBatch
        /// </summary>
        public Task<bool> DirectApplyIndexUpdateBatch(Immutable<IDictionary<IIndexableGrain, IList<IMemberUpdate>>> iUpdates, bool isUnique, IndexMetaData idxMetaData, SiloAddress siloAddress = null)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// DirectApplyIndexUpdate is not supported on ActiveHashIndexPartitionedPerSiloImpl,
        /// because it will be skipped via IndexExtensions.ApplyIndexUpdate
        /// </summary>
        public Task<bool> DirectApplyIndexUpdate(IIndexableGrain g, Immutable<IMemberUpdate> iUpdate, bool isUniqueIndex, IndexMetaData idxMetaData, SiloAddress siloAddress)
        {
            throw new NotSupportedException();
        }

        private static GrainId GetGrainID(string indexName)
        {
            return IndexExtensions.GetSystemTargetGrainId(IndexingConstants.HASH_INDEX_PARTITIONED_PER_SILO_BUCKET_SYSTEM_TARGET_TYPE_CODE,
                                                          IndexUtils.GetIndexGrainID(typeof(V), indexName));
        }

        public Task<bool> IsUnique()
        {
            return Task.FromResult(false);
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

        public async Task Dispose()
        {
            _status = IndexStatus.Disposed;
            GrainId grainID = GetGrainID(IndexUtils.GetIndexNameFromIndexGrain(this));

            // Get and Dispose() all silos
            Dictionary<SiloAddress, SiloStatus> hosts = await SiloUtils.GetHosts(base.GrainFactory, true);
            await Task.WhenAll(hosts.Keys.Select(sa => this.indexManager.GetSystemTarget<IActiveHashIndexPartitionedPerSiloBucket>(grainID, sa).Dispose()));
        }

        public Task<bool> IsAvailable() => Task.FromResult(_status == IndexStatus.Available);

        async Task<IOrleansQueryResult<IIndexableGrain>> IIndexInterface.Lookup(object key)
        {
            logger.Trace($"Eager index lookup called for key = {key}");

            //get all silos
            Dictionary<SiloAddress, SiloStatus> hosts = await SiloUtils.GetHosts(base.GrainFactory, true);
            IEnumerable<IIndexableGrain>[] queriesToSilos = await Task.WhenAll(GetResultQueries(hosts, key));
            return new OrleansQueryResult<V>(queriesToSilos.SelectMany(res => res.Select(e => e.AsReference<V>())).ToList());
        }

        public async Task<IOrleansQueryResult<V>> Lookup(K key) => (IOrleansQueryResult<V>)await ((IIndexInterface)this).Lookup(key);

        private ISet<Task<IOrleansQueryResult<IIndexableGrain>>> GetResultQueries(Dictionary<SiloAddress, SiloStatus> hosts, object key)
        {
            ISet<Task<IOrleansQueryResult<IIndexableGrain>>> queriesToSilos = new HashSet<Task<IOrleansQueryResult<IIndexableGrain>>>();

            int i = 0;
            GrainId grainID = GetGrainID(IndexUtils.GetIndexNameFromIndexGrain(this));
            foreach (SiloAddress siloAddress in hosts.Keys)
            {
                //query each silo
                queriesToSilos.Add(this.indexManager.GetSystemTarget<IActiveHashIndexPartitionedPerSiloBucket>(
                    grainID,
                    siloAddress
                ).Lookup(/*result, */key)); //TODO: because of a bug in OrleansStream, a SystemTarget cannot work with streams. It should be fixed later.
                ++i;
            }

            return queriesToSilos;
        }

        public Task Lookup(IOrleansQueryResultStream<V> result, K key)
        {
            return ((IIndexInterface)this).Lookup(result.Cast<IIndexableGrain>(), key);
        }

        async Task IIndexInterface.Lookup(IOrleansQueryResultStream<IIndexableGrain> result, object key)
        {
            logger.Trace($"Streamed index lookup called for key = {key}");

            //get all silos
            Dictionary<SiloAddress, SiloStatus> hosts = await SiloUtils.GetHosts(base.GrainFactory, true);

            ISet<Task<IOrleansQueryResult<IIndexableGrain>>> queriesToSilos = GetResultQueries(hosts, key);

            //TODO: After fixing the problem with OrleansStream, this part is not needed anymore. vv2 find out what that problem is
            while (queriesToSilos.Count > 0)
            {
                // Identify the first task that completes.
                Task<IOrleansQueryResult<IIndexableGrain>> firstFinishedTask = await Task.WhenAny(queriesToSilos);

                // ***Remove the selected task from the list so that you don't
                // process it more than once.
                queriesToSilos.Remove(firstFinishedTask);

                // Await the completed task.
                IOrleansQueryResult<IIndexableGrain> partialResult = await firstFinishedTask;

                await result.OnNextBatchAsync(partialResult);
            }
            await result.OnCompletedAsync();
        }
    }
}
