using System.Linq;

namespace Orleans.Indexing
{
    internal static class HashIndexBucketUtils
    {
        /// <summary>
        /// This method contains the common functionality for updating the hash-index bucket.
        /// </summary>
        /// <typeparam name="K">key type</typeparam>
        /// <typeparam name="V">value type</typeparam>
        /// <param name="updatedGrain">the updated grain that is being indexed</param>
        /// <param name="iUpdate">the update information</param>
        /// <param name="state">the index bucket to be updated</param>
        /// <param name="isUniqueIndex">a flag to indicate whether the hash-index has a uniqueness constraint</param>
        /// <param name="idxMetaData">the index metadata</param>
        internal static bool UpdateBucket<K, V>(V updatedGrain, IMemberUpdate iUpdate, HashIndexBucketState<K, V> state, bool isUniqueIndex, IndexMetaData idxMetaData) where V : IIndexableGrain
            => UpdateBucket(updatedGrain, iUpdate, state, isUniqueIndex, idxMetaData, out K befImg, out HashIndexSingleBucketEntry<V> befEntry, out bool fixIndexUnavailableOnDelete);

        /// <summary>
        /// This method contains the common functionality for updating the hash-index bucket.
        /// </summary>
        /// <typeparam name="K">key type</typeparam>
        /// <typeparam name="V">value type</typeparam>
        /// <param name="updatedGrain">the updated grain that is being indexed</param>
        /// <param name="update">the update information</param>
        /// <param name="state">the index bucket to be updated</param>
        /// <param name="isUniqueIndex">a flag to indicate whether the hash-index has a uniqueness constraint</param>
        /// <param name="idxMetaData">the index metadata</param>
        /// <param name="befImg">output parameter: the before-image</param>
        /// <param name="befEntry">output parameter: the index entry containing the before-image</param>
        /// <param name="fixIndexUnavailableOnDelete">output parameter: this variable determines whether
        ///             the index was still unavailable when we received a delete operation</param>
        internal static bool UpdateBucket<K, V>(V updatedGrain, IMemberUpdate update, HashIndexBucketState<K, V> state, bool isUniqueIndex, IndexMetaData idxMetaData, out K befImg,
                                                out HashIndexSingleBucketEntry<V> befEntry, out bool fixIndexUnavailableOnDelete) where V : IIndexableGrain
        {
            fixIndexUnavailableOnDelete = false;
            befImg = default(K);
            befEntry = null;

            bool isTentativeUpdate = isUniqueIndex && (update is MemberUpdateTentative);
            IndexOperationType opType = update.GetOperationType();
            HashIndexSingleBucketEntry<V> aftEntry;
            if (opType == IndexOperationType.Update)
            {
                befImg = (K)update.GetBeforeImage();
                K aftImg = (K)update.GetAfterImage();
                if (state.IndexMap.TryGetValue(befImg, out befEntry) && befEntry.Values.Contains(updatedGrain))
                {   //Delete and Insert
                    if (state.IndexMap.TryGetValue(aftImg, out aftEntry))
                    {
                        if (aftEntry.Values.Contains(updatedGrain))
                        {
                            if (isTentativeUpdate)
                            {
                                aftEntry.SetTentativeInsert();
                            }
                            else
                            {
                                aftEntry.ClearTentativeFlag();
                                befEntry.Remove(updatedGrain, isTentativeUpdate, isUniqueIndex);
                            }
                        }
                        else
                        {
                            if (isUniqueIndex && aftEntry.Values.Count > 0)
                            {
                                throw new UniquenessConstraintViolatedException(string.Format(
                                        "The uniqueness property of index is violated after an update operation for before-image = {0}, after-image = {1} and grain = {2}",
                                        befImg, aftImg, updatedGrain.GetPrimaryKey()));
                            }
                            befEntry.Remove(updatedGrain, isTentativeUpdate, isUniqueIndex);
                            aftEntry.Add(updatedGrain, isTentativeUpdate, isUniqueIndex);
                        }
                    }
                    else
                    {
                        aftEntry = new HashIndexSingleBucketEntry<V>();
                        befEntry.Remove(updatedGrain, isTentativeUpdate, isUniqueIndex);
                        aftEntry.Add(updatedGrain, isTentativeUpdate, isUniqueIndex);
                        state.IndexMap.Add(aftImg, aftEntry);
                    }
                }
                else
                {
                    if (idxMetaData.IsChainedBuckets)
                        return false; //not found in this bucket
                    //Insert
                    if (state.IndexMap.TryGetValue(aftImg, out aftEntry))
                    {
                        if (!aftEntry.Values.Contains(updatedGrain))
                        {
                            if (isUniqueIndex && aftEntry.Values.Count > 0)
                            {
                                throw new UniquenessConstraintViolatedException(string.Format("The uniqueness property of index is violated after an update operation for (not found before-image = {0}), after-image = {1} and grain = {2}", befImg, aftImg, updatedGrain.GetPrimaryKey()));
                            }
                            aftEntry.Add(updatedGrain, isTentativeUpdate, isUniqueIndex);
                        }
                        else if (isTentativeUpdate)
                        {
                            aftEntry.SetTentativeInsert();
                        }
                        else
                        {
                            aftEntry.ClearTentativeFlag();
                        }
                    }
                    else
                    {
                        if (idxMetaData.IsCreatingANewBucketNecessary(state.IndexMap.Count()))
                        {
                            return false;
                        }
                        aftEntry = new HashIndexSingleBucketEntry<V>();
                        aftEntry.Add(updatedGrain, isTentativeUpdate, isUniqueIndex);
                        state.IndexMap.Add(aftImg, aftEntry);
                    }
                }
            }
            else if (opType == IndexOperationType.Insert)
            { // Insert
                K aftImg = (K)update.GetAfterImage();
                if (state.IndexMap.TryGetValue(aftImg, out aftEntry))
                {
                    if (!aftEntry.Values.Contains(updatedGrain))
                    {
                        if (isUniqueIndex && aftEntry.Values.Count > 0)
                        {
                            throw new UniquenessConstraintViolatedException(string.Format("The uniqueness property of index is violated after an insert operation for after-image = {0} and grain = {1}",
                                                                                           aftImg, updatedGrain.GetPrimaryKey()));
                        }
                        aftEntry.Add(updatedGrain, isTentativeUpdate, isUniqueIndex);
                    }
                    else if (isTentativeUpdate)
                    {
                        aftEntry.SetTentativeInsert();
                    }
                    else
                    {
                        aftEntry.ClearTentativeFlag();
                    }
                }
                else
                {
                    if (idxMetaData.IsCreatingANewBucketNecessary(state.IndexMap.Count()))
                    {
                        return false;  //the bucket is full
                    }
                    aftEntry = new HashIndexSingleBucketEntry<V>();
                    aftEntry.Add(updatedGrain, isTentativeUpdate, isUniqueIndex);
                    state.IndexMap.Add(aftImg, aftEntry);
                }
            }
            else if (opType == IndexOperationType.Delete)
            { // Delete
                befImg = (K)update.GetBeforeImage();

                if (state.IndexMap.TryGetValue(befImg, out befEntry) && befEntry.Values.Contains(updatedGrain))
                {
                    befEntry.Remove(updatedGrain, isTentativeUpdate, isUniqueIndex);
                    if (state.IndexStatus != IndexStatus.Available)
                    {
                        fixIndexUnavailableOnDelete = true;
                    }
                }
                else if (idxMetaData.IsChainedBuckets)
                {
                    return false; //not found in this bucket
                }
            }
            return true;
        }
    }
}
