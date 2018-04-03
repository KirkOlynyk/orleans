using System;
using Orleans.Runtime;
using System.Reflection;
using Orleans.Streams;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq.Expressions;

namespace Orleans.Indexing
{
    /// <summary>
    /// A utility class for the index operations
    /// </summary>
    internal class IndexFactory : IIndexFactory
    {
        private IndexingManager indexingManager;
        private IGrainFactory grainFactory;
        private IRuntimeClient runtimeClient;

        internal IndexFactory(IndexingManager im, IGrainFactory gf, IRuntimeClient rtc)
        {
            this.indexingManager = im;
            this.grainFactory = gf;
            this.runtimeClient = rtc;
        }

        #region IIndexFactory

        /// <summary>
        /// This method queries the active grains for the given grain interface and the filter expression. The filter
        /// expression should contain an indexed field.
        /// </summary>
        /// <typeparam name="TIGrain">the given grain interface type to query over its active instances</typeparam>
        /// <param name="gf">the grain factory instance</param>
        /// <param name="filterExpr">the filter expression of the query</param>
        /// <param name="queryResultObserver">the observer object to be called on every grain found for the query</param>
        /// <returns>the result of the query</returns>
        public Task GetActiveGrains<TIGrain, TProperties>(Expression<Func<TProperties, bool>> filterExpr,
                                IAsyncBatchObserver<TIGrain> queryResultObserver) where TIGrain : IIndexableGrain
            => this.GetActiveGrains<TIGrain, TProperties>().Where(filterExpr).ObserveResults(queryResultObserver);

        /// <summary>
        /// This method queries the active grains for the given grain interface and the filter expression. The filter
        /// expression should contain an indexed field.
        /// </summary>
        /// <typeparam name="TIGrain">the given grain interface type to query over its active instances</typeparam>
        /// <param name="gf">the grain factory instance</param>
        /// <param name="streamProvider">the stream provider for the query results</param>
        /// <param name="filterExpr">the filter expression of the query</param>
        /// <param name="queryResultObserver">the observer object to be called on every grain found for the query</param>
        /// <returns>the result of the query</returns>
        public Task GetActiveGrains<TIGrain, TProperties>(IStreamProvider streamProvider,
                                Expression<Func<TProperties, bool>> filterExpr, IAsyncBatchObserver<TIGrain> queryResultObserver) where TIGrain : IIndexableGrain
            => this.GetActiveGrains<TIGrain, TProperties>(streamProvider).Where(filterExpr).ObserveResults(queryResultObserver);

        /// <summary>
        /// This method queries the active grains for the given grain interface.
        /// </summary>
        /// <typeparam name="TIGrain">the given grain interface type to query over its active instances</typeparam>
        /// <param name="gf">the grain factory instance</param>
        /// <returns>the query to lookup all active grains of a given type</returns>
        public IOrleansQueryable<TIGrain, TProperty> GetActiveGrains<TIGrain, TProperty>() where TIGrain : IIndexableGrain
            => this.GetActiveGrains<TIGrain, TProperty>(this.indexingManager.ServiceProvider.GetRequiredServiceByName<IStreamProvider>(IndexingConstants.INDEXING_STREAM_PROVIDER_NAME));

        /// <summary>
        /// This method queries the active grains for the given grain interface.
        /// </summary>
        /// <typeparam name="TIGrain">the given grain interface type to query over its active instances</typeparam>
        /// <param name="gf">the grain factory instance</param>
        /// <param name="streamProvider">the stream provider for the query results</param>
        /// <returns>the query to lookup all active grains of a given type</returns>
        public IOrleansQueryable<TIGrain, TProperty> GetActiveGrains<TIGrain, TProperty>(IStreamProvider streamProvider) where TIGrain : IIndexableGrain
            => new QueryActiveGrainsNode<TIGrain, TProperty>(this, streamProvider);

        /// <summary>
        /// Gets an IndexInterface<K,V> given its name
        /// </summary>
        /// <typeparam name="K">key type of the index</typeparam>
        /// <typeparam name="V">value type of the index, which is the grain being indexed</typeparam>
        /// <param name="indexName">the name of the index, which is the identifier of the index</param>
        /// <returns>the IndexInterface<K,V> with the specified name</returns>
        public IIndexInterface<K, V> GetIndex<K, V>(string indexName) where V : IIndexableGrain
            => (IIndexInterface<K, V>)this.GetIndex(typeof(V), indexName);

        /// <summary>
        /// Gets an IndexInterface given its name and grain interface type
        /// </summary>
        /// <param name="indexName">the name of the index, which is the identifier of the index<</param>
        /// <param name="iGrainType">the grain interface type that is being indexed</param>
        /// <returns>the IndexInterface with the specified name on the given grain interface type</returns>
        public IIndexInterface GetIndex(Type iGrainType, string indexName)
        {
            if (GetIndexes(iGrainType).TryGetValue(indexName, out Tuple<object, object, object> index))
            {
                return (IIndexInterface)index.Item1;
            }

            // It should never happen that the indexes are not loaded if the index is registered in the index registry
            throw new Exception(string.Format("Index \"{0}\" does not exist for {1}.", indexName, iGrainType)); //vv2 add IndexingException class
        }

        #endregion IIndexFactory

        #region internal functions

        /// <summary>
        /// Provides the index information for a given grain interface type.
        /// </summary>
        /// <param name="iGrainType">The target grain interface type</param>
        /// <returns>the index information for the given grain type T.
        /// The index information is a dictionary from indexIDs defined
        /// on a grain interface to a triple. The triple consists of:
        /// 1) the index object (that implements IndexInterface,
        /// 2) the IndexMetaData object for this index, and
        /// 3) the IndexUpdateGenerator instance for this index.
        /// This triple is untyped, because IndexInterface, IndexMetaData
        /// and IndexUpdateGenerator types are not visible in this project.
        /// 
        /// This method returns an empty dictionary if the OrleansIndexing 
        /// project is not available.</returns>
        internal IDictionary<string, Tuple<object, object, object>> GetIndexes(Type iGrainType)
        {
            return this.indexingManager.Indexes.TryGetValue(iGrainType, out IDictionary<string, Tuple<object, object, object>> indexes)
                ? indexes
                : new Dictionary<string, Tuple<object, object, object>>();
        }

        /// <summary>
        /// This is a helper method for creating an index on a field of an actor.
        /// </summary>
        /// <param name="gf">The current instance of IGrainFactory</param>
        /// <param name="idxType">The type of index to be created</param>
        /// <param name="indexName">The index name to be created</param>
        /// <param name="isUniqueIndex">Determines whether this is a unique index that needs to be created</param>
        /// <param name="isEager">Determines whether updates to this index should be applied eagerly or not</param>
        /// <param name="maxEntriesPerBucket">Determines the maximum number of entries in
        /// each bucket of a distributed index, if this index type is a distributed one.</param>
        /// <param name="indexedProperty">the PropertyInfo object for the indexed field.
        /// This object helps in creating a default instance of IndexUpdateGenerator.</param>
        /// <returns>A triple that consists of:
        /// 1) the index object (that implements IndexInterface
        /// 2) the IndexMetaData object for this index, and
        /// 3) the IndexUpdateGenerator instance for this index.
        /// This triple is untyped, because IndexInterface, IndexMetaData
        /// and IndexUpdateGenerator types are not visible in the core project.</returns>
        internal Tuple<object, object, object> CreateIndex(Type idxType, string indexName, bool isUniqueIndex, bool isEager, int maxEntriesPerBucket, PropertyInfo indexedProperty)
        {
            Type iIndexType = idxType.GetGenericType(typeof(IIndexInterface<,>));
            if (iIndexType == null)
            {
                throw new NotSupportedException(string.Format("Adding an index that does not implement IndexInterface<K,V> is not supported yet. Your requested index ({0}) is invalid.", idxType.ToString()));
            }

            Type[] indexTypeArgs = iIndexType.GetGenericArguments();
            Type keyType = indexTypeArgs[0];
            Type grainType = indexTypeArgs[1];

            IIndexInterface index;
            if (typeof(IGrain).IsAssignableFrom(idxType))
            {
                index = (IIndexInterface)this.indexingManager.GrainFactory.GetGrain(this.indexingManager.GrainTypeResolver,
                                                                                IndexUtils.GetIndexGrainID(grainType, indexName), idxType, idxType);

                var idxImplType = this.indexingManager.CachedTypeResolver.ResolveType(TypeCodeMapper.GetImplementation(this.indexingManager.RuntimeClient, idxType).GrainClass);
                if (idxImplType.IsGenericTypeDefinition)
                    idxImplType = idxImplType.MakeGenericType(iIndexType.GetGenericArguments());

                var initPerSiloMethodInfo = idxImplType.GetMethod("InitPerSilo", BindingFlags.Static | BindingFlags.Public);
                if (initPerSiloMethodInfo != null)  // Static method so cannot use an interface
                {
                    var initPerSiloMethod = (Action<IndexingManager, string, bool>)Delegate.CreateDelegate(
                                            typeof(Action<IndexingManager, string, bool>), initPerSiloMethodInfo);
                    initPerSiloMethod(this.indexingManager, indexName, isUniqueIndex);
                }
            }
            else 
            {
                index = idxType.IsClass
                    ? (IIndexInterface)Activator.CreateInstance(idxType, this.indexingManager.ServiceProvider, indexName, isUniqueIndex)
                    : throw new Exception(string.Format("{0} is neither a grain nor a class. Index \"{1}\" cannot be created.", idxType, indexName));
            }

            return Tuple.Create((object)index, (object)new IndexMetaData(idxType, isUniqueIndex, isEager, maxEntriesPerBucket), (object)CreateIndexUpdateGenFromProperty(indexedProperty));
        }

        internal static void RegisterIndexWorkflowQueues(IndexingManager indexingManager, Type iGrainType, Type grainImplType)
        {
            for (int i = 0; i < IndexWorkflowQueueBase.NUM_AVAILABLE_INDEX_WORKFLOW_QUEUES; ++i)
            {
                bool isAssignable = typeof(IIndexableGrainFaultTolerant).IsAssignableFrom(grainImplType);
                indexingManager.Silo.RegisterSystemTarget(new IndexWorkflowQueueSystemTarget(indexingManager, iGrainType, i, isAssignable));
                indexingManager.Silo.RegisterSystemTarget(new IndexWorkflowQueueHandlerSystemTarget(indexingManager, iGrainType, i, isAssignable));
            }
        }

        #endregion internal functions

        #region private functions

        private static IIndexUpdateGenerator CreateIndexUpdateGenFromProperty(PropertyInfo indexedProperty)
            => new IndexUpdateGenerator(indexedProperty);

        #endregion private functions

    }
}
