using Orleans.Concurrency;
using System;
using System.Linq;

namespace Orleans.Indexing
{
    /// <summary>
    /// The meta data that is stored beside the index
    /// </summary>
    [Serializable]
    public class IndexMetaData
    {
        private Type _indexType;
        private bool _isUniqueIndex;
        private bool _isEager;
        private int _maxEntriesPerBucket;

        /// <summary>
        /// Constructs an IndexMetaData, which currently only consists of the type of the index
        /// </summary>
        /// <param name="indexType">Type of the index implementation class.</param>
        /// <param name="isEager">Determines whether the index should be updated eagerly upon any change in the indexed grains. Otherwise,
        /// the update propagation happens lazily after applying the update to the grain itself.</param>
        /// <param name="isUniqueIndex">Determines whether the index should maintain a uniqueness constraint.</param>
        /// <param name="maxEntriesPerBucket">The maximum number of entries that should be stored in each bucket of a distributed index. This
        /// option is only considered if the index is a distributed index. Use -1 to declare no limit.</param>
        public IndexMetaData(Type indexType, bool isUniqueIndex, bool isEager, int maxEntriesPerBucket)
        {
            this._indexType = indexType;
            this._isUniqueIndex = isUniqueIndex;
            this._isEager = isEager;
            this._maxEntriesPerBucket = maxEntriesPerBucket;
        }

#if false // TODO not used? IsStatelessWorker
        /// <summary>
        /// Determines whether the index grain is a stateless worker or not. This piece of information can impact the relationship
        /// between index handlers and the index. 
        /// </summary>
        /// <returns>the result of whether the current index is
        /// a stateless worker or not</returns>
        public bool IsIndexStatelessWorker => IsStatelessWorker(Type.GetType(TypeCodeMapper.GetImplementation(this._indexType).GrainClass));

        /// <summary>
        /// A helper function that determines whether a given grain type is annotated with StatelessWorker annotation or not.
        /// </summary>
        /// <param name="grainType">the grain type to be tested</param>
        /// <returns>true if the grain type has StatelessWorker annotation, otherwise false.</returns>
        private static bool IsStatelessWorker(Type grainType)
        {
            bool hasStatelessWorkerAttribute(Type t) => t.GetCustomAttributes(typeof(StatelessWorkerAttribute), true).Length > 0;
            return hasStatelessWorkerAttribute(grainType) || grainType.GetInterfaces().Any(i => hasStatelessWorkerAttribute(i));
        }
#endif

        public bool IsUniqueIndex => this._isUniqueIndex;

        public bool IsEager => this._isEager;

        public bool IsChainedBuckets => this._maxEntriesPerBucket > 0;

        internal bool IsCreatingANewBucketNecessary(int currentSize) => this.IsChainedBuckets && currentSize >= this._maxEntriesPerBucket;
    }
}
