using System;
using System.Threading.Tasks;
using Orleans.Runtime;
using System.Linq.Expressions;
using System.Reflection;
using Orleans.Streams;

namespace Orleans.Indexing
{
    /// <summary>
    /// A utility class for the index operations
    /// </summary>
    public static class IndexFactory
    {
#if false //vv2 unused? overloads of GetActiveGrains
        /// <summary>
        /// This method queries the active grains for the given
        /// grain interface and the filter expression. The filter
        /// expression should contain an indexed field.
        /// </summary>
        /// <typeparam name="TIGrain">the given grain interface
        /// type to query over its active instances</typeparam>
        /// <param name="gf">the grain factory instance</param>
        /// <param name="filterExpr">the filter expression of the query</param>
        /// <param name="queryResultObserver">the observer object to be called
        /// on every grain found for the query</param>
        /// <returns>the result of the query</returns>
        public static Task GetActiveGrains<TIGrain, TProperties>(this IGrainFactory gf, Expression<Func<TProperties, bool>> filterExpr,
                                IAsyncBatchObserver<TIGrain> queryResultObserver) where TIGrain : IIndexableGrain
        {
            return gf.GetActiveGrains<TIGrain, TProperties>()
                     .Where(filterExpr)
                     .ObserveResults(queryResultObserver);
        }

        /// <summary>
        /// This method queries the active grains for the given
        /// grain interface and the filter expression. The filter
        /// expression should contain an indexed field.
        /// </summary>
        /// <typeparam name="TIGrain">the given grain interface
        /// type to query over its active instances</typeparam>
        /// <param name="gf">the grain factory instance</param>
        /// <param name="streamProvider">the stream provider for the query results</param>
        /// <returns>the query to lookup all active grains of a given type</returns>
        /// <param name="filterExpr">the filter expression of the query</param>
        /// <param name="queryResultObserver">the observer object to be called
        /// on every grain found for the query</param>
        /// <returns>the result of the query</returns>
        public static Task GetActiveGrains<TIGrain, TProperties>(this IGrainFactory gf, IStreamProvider streamProvider,
                                Expression<Func<TProperties, bool>> filterExpr, IAsyncBatchObserver<TIGrain> queryResultObserver) where TIGrain : IIndexableGrain
        {
            return gf.GetActiveGrains<TIGrain, TProperties>(streamProvider)
                     .Where(filterExpr)
                     .ObserveResults(queryResultObserver);
        }
#endif

#if false //vv2 unused? GetActiveGrains overload with no IStreamProvider
        /// <summary>
        /// This method queries the active grains for the given
        /// grain interface.
        /// </summary>
        /// <typeparam name="TIGrain">the given grain interface
        /// type to query over its active instances</typeparam>
        /// <param name="gf">the grain factory instance</param>
        /// <returns>the query to lookup all active grains of a given type</returns>
        public static IOrleansQueryable<TIGrain, TProperty> GetActiveGrains<TIGrain, TProperty>(this IGrainFactory gf) where TIGrain : IIndexableGrain
        {
            return GetActiveGrains<TIGrain, TProperty>(gf, GrainClient.GetStreamProvider(IndexingConstants.INDEXING_STREAM_PROVIDER_NAME));
        }
#endif

        /// <summary>
        /// This method queries the active grains for the given
        /// grain interface.
        /// </summary>
        /// <typeparam name="TIGrain">the given grain interface
        /// type to query over its active instances</typeparam>
        /// <param name="gf">the grain factory instance</param>
        /// <param name="streamProvider">the stream provider for the query results</param>
        /// <returns>the query to lookup all active grains of a given type</returns>
        public static IOrleansQueryable<TIGrain, TProperty> GetActiveGrains<TIGrain, TProperty>(this IGrainFactory gf, IStreamProvider streamProvider) where TIGrain : IIndexableGrain
        {
            return new QueryActiveGrainsNode<TIGrain, TProperty>(gf, streamProvider);
        }

        /// <summary>
        /// Gets an IndexInterface<K,V> given its name
        /// </summary>
        /// <typeparam name="K">key type of the index</typeparam>
        /// <typeparam name="V">value type of the index, which is
        /// the grain being indexed</typeparam>
        /// <param name="indexName">the name of the index, which
        /// is the identifier of the index</param>
        /// <returns>the IndexInterface<K,V> with the specified name</returns>
        public static IIndexInterface<K, V> GetIndex<K, V>(this IGrainFactory gf, string indexName) where V : IIndexableGrain
        {
            return IndexHandler.GetIndex<K, V>(indexName);
        }

        /// <summary>
        /// Gets an IndexInterface given its name and grain interface type
        /// </summary>
        /// <param name="indexName">the name of the index, which
        /// is the identifier of the index<</param>
        /// <param name="iGrainType">the grain interface type
        /// that is being indexed</param>
        /// <returns>the IndexInterface with the specified name on the
        /// given grain interface type</returns>
        internal static IIndexInterface GetIndex(this IGrainFactory gf, string indexName, Type iGrainType)
        {
            return IndexHandler.GetIndex(iGrainType, indexName);
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
        internal static Tuple<object, object, object> CreateIndex(IndexingManager indexingManager, Type idxType,
                    string indexName, bool isUniqueIndex, bool isEager, int maxEntriesPerBucket, PropertyInfo indexedProperty)
        {
            Type iIndexType = idxType.GetGenericType(typeof(IIndexInterface<,>));
            if (iIndexType != null)
            {
                Type[] indexTypeArgs = iIndexType.GetGenericArguments();
                Type keyType = indexTypeArgs[0];
                Type grainType = indexTypeArgs[1];

                IIndexInterface index;
                if (typeof(IGrain).IsAssignableFrom(idxType))
                {
                    index = (IIndexInterface)indexingManager.GrainFactory.GetGrain(indexingManager.GrainTypeResolver,
                                                                                   IndexUtils.GetIndexGrainID(grainType, indexName), idxType, idxType);

                    Type idxImplType = indexingManager.CachedTypeResolver.ResolveType(TypeCodeMapper.GetImplementation(indexingManager.RuntimeClient, idxType).GrainClass);
                    if (idxImplType.IsGenericTypeDefinition)
                        idxImplType = idxImplType.MakeGenericType(iIndexType.GetGenericArguments());

                    MethodInfo initPerSiloMethodInfo = idxImplType.GetMethod("InitPerSilo", BindingFlags.Static | BindingFlags.Public);
                    if (initPerSiloMethodInfo != null)  // Static method so cannot use an interface
                    {
                        var initPerSiloMethod = (Action<IndexingManager, string, bool>)Delegate.CreateDelegate(
                                                typeof(Action<IndexingManager, string, bool>), initPerSiloMethodInfo);
                        initPerSiloMethod(indexingManager, indexName, isUniqueIndex);
                    }
                }
                else if (idxType.IsClass)
                {
                    index = (IIndexInterface)Activator.CreateInstance(idxType, indexingManager.ServiceProvider, indexName, isUniqueIndex);
                }
                else
                {
                    throw new Exception(string.Format("{0} is neither a grain nor a class. Index \"{1}\" cannot be created.", idxType, indexName));
                }

                return Tuple.Create((object)index, (object)new IndexMetaData(idxType, isUniqueIndex, isEager, maxEntriesPerBucket), (object)CreateIndexUpdateGenFromProperty(indexedProperty));
            }
            else
            {
                throw new NotSupportedException(string.Format("Adding an index that does not implement IndexInterface<K,V> is not supported yet. Your requested index ({0}) is invalid.", idxType.ToString()));
            }
        }

        private static IIndexUpdateGenerator CreateIndexUpdateGenFromProperty(PropertyInfo indexedProperty)
        {
            return new IndexUpdateGenerator(indexedProperty);
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
    }
}
