using System;

namespace Orleans.Indexing
{
    /// <summary>
    /// The attribute for declaring the property fields of an
    /// indexed grain interface to have an "Active Index".
    /// 
    /// An "Active Index" indexes all the grains that are 
    /// currently active in the silos.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class ActiveIndexAttribute : IndexAttribute
    {
        /// <summary>
        /// The default constructor for ActiveIndex.
        /// </summary>
        public ActiveIndexAttribute() : this(false)
        {
        }

        /// <summary>
        /// The constructor for ActiveIndex.
        /// </summary>
        /// <param name="isEager">Determines whether the index should be
        /// updated eagerly upon any change in the indexed grains. Otherwise,
        /// the update propagation happens lazily after applying the update
        /// to the grain itself.</param>
        public ActiveIndexAttribute(bool isEager) : this(Indexing.ActiveIndexType.HashIndexSingleBucket, isEager)
        {
        }

        /// <summary>
        /// The full-option constructor for ActiveIndex.
        /// </summary>
        /// <param name="type">The index type for the active index</param>
        /// <param name="isEager">Determines whether the index should be
        /// updated eagerly upon any change in the indexed grains. Otherwise,
        /// the update propagation happens lazily after applying the update
        /// to the grain itself.</param>
        /// <param name="maxEntriesPerBucket">The maximum number of entries
        /// that should be stored in each bucket of a distributed index. This
        /// option is only considered if the index is a distributed index.
        /// Use -1 to declare no limit.</param>
        public ActiveIndexAttribute(ActiveIndexType type, bool isEager = false, int maxEntriesPerBucket = -1)
        {
            switch (type)
            {
                case Indexing.ActiveIndexType.HashIndexSingleBucket:
                    this.IndexType = typeof(IActiveHashIndexSingleBucket<,>);
                    break;
                case Indexing.ActiveIndexType.HashIndexPartitionedByKeyHash:
                    this.IndexType = typeof(ActiveHashIndexPartitionedPerKey<,>);
                    break;
                case Indexing.ActiveIndexType.HashIndexPartitionedBySilo:
                    this.IndexType = typeof(IActiveHashIndexPartitionedPerSilo<,>);
                    break;
                default:
                    this.IndexType = typeof(IActiveHashIndexSingleBucket<,>);
                    break;
            }
            this.IsEager = isEager;
            //Active Index cannot be defined as unique
            //Suppose there's a unique Active Index over persistent objects.
            //The activation of an initialized object could create a conflict in
            //the Active Index. E.g., there's an active player PA with email foo and
            //a non-active persistent player PP with email foo. An attempt to
            //activate PP will cause a violation of the Active Index on email.
            //This implies we should disallow such indexes.
            this.IsUnique = false;
            this.MaxEntriesPerBucket = maxEntriesPerBucket;
        }
    }
}
