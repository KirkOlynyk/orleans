using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Concurrency;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Indexing
{
    /// <summary>
    /// A simple implementation of a direct storage managed index (i.e., without caching)
    /// </summary>
    /// <typeparam name="K">type of hash-index key</typeparam>
    /// <typeparam name="V">type of grain that is being indexed</typeparam>
    [Reentrant]
    //[StatelessWorker]
    //TODO: because of a bug in OrleansStreams (that streams cannot work with stateless grains), this grain cannot be StatelessWorker. It should be fixed later.
    //TODO: basically, this class does not even need to be a grain, but it's not possible to call a SystemTarget from a non-grain
    public class DirectStorageManagedIndexImpl<K, V> : Grain, IDirectStorageManagedIndex<K, V> where V : class, IIndexableGrain
    {
        private IStorageProvider _storageProvider;
        private string grainImplClass;

        private string _indexName;
        private string _indexedField;
        //private bool _isUnique; //TODO: missing support for the uniqueness feature

        internal IndexingManager IndexingManager { get { return IndexingManager.GetIndexingManager(ref __indexingManager, base.ServiceProvider); } }
        private IndexingManager __indexingManager;

        //vv2 private static readonly Logger logger = LogManager.GetLogger(string.Format("HashIndexPartitionedPerKey<{0},{1}>", typeof(K).Name, typeof(V).Name), LoggerType.Grain);

        public override Task OnActivateAsync()
        {
            _indexName = IndexUtils.GetIndexNameFromIndexGrain(this);
            _indexedField = _indexName.Substring(2);
            //_isUnique = isUniqueIndex; //TODO: missing support for the uniqueness feature
            return base.OnActivateAsync();
        }

        public Task<bool> DirectApplyIndexUpdateBatch(Immutable<IDictionary<IIndexableGrain, IList<IMemberUpdate>>> iUpdates, bool isUnique, IndexMetaData idxMetaData, SiloAddress siloAddress = null)
        {
            return Task.FromResult(true);
        }

        public Task<bool> DirectApplyIndexUpdate(IIndexableGrain g, Immutable<IMemberUpdate> iUpdate, bool isUniqueIndex, IndexMetaData idxMetaData, SiloAddress siloAddress)
        {
            return Task.FromResult(true);
        }

        public async Task Lookup(IOrleansQueryResultStream<V> result, K key)
        {
            var res = await LookupGrainReferences(key);
            await result.OnNextBatchAsync(res);
            await result.OnCompletedAsync();
        }

        private async Task<List<V>> LookupGrainReferences(K key)
        {
            EnsureStorageProvider();

            dynamic indexableStorageProvider = _storageProvider;

            List<GrainReference> resultReferences = await indexableStorageProvider.Lookup<K>(grainImplClass, _indexedField, key);
            return resultReferences.Select(grain => this.IndexingManager.RuntimeClient.InternalGrainFactory.Cast<V>(grain)).ToList();
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
            return Task.CompletedTask;
        }

        public Task<bool> IsAvailable()
        {
            return Task.FromResult(true);
        }

        Task IIndexInterface.Lookup(IOrleansQueryResultStream<IIndexableGrain> result, object key)
        {
            return Lookup(result.Cast<V>(), (K)key);
        }

        public async Task<IOrleansQueryResult<V>> Lookup(K key)
        {
            return new OrleansQueryResult<V>(await LookupGrainReferences(key));
        }

        async Task<IOrleansQueryResult<IIndexableGrain>> IIndexInterface.Lookup(object key)
        {
            return await Lookup((K)key);
        }

        private void EnsureStorageProvider()
        {
            if (_storageProvider == null)
            {
                var implementation = TypeCodeMapper.GetImplementation(this.IndexingManager.RuntimeClient, typeof(V));
                Type implType;
                if (implementation == null || (grainImplClass = implementation.GrainClass) == null ||
                        !this.IndexingManager.CachedTypeResolver.TryResolveType(grainImplClass, out implType))
                {
                    throw new Exception("The grain implementation class " + implementation.GrainClass + " for grain interface " + TypeUtils.GetFullName(typeof(V)) + " was not resolved.");
                }
                _storageProvider = null; //vv2err Catalog.SetupStorageProvider not implementable: this.IndexingManager.RuntimeClient.Catalog.SetupStorageProvider(implType);
            }
        }
    }
}
