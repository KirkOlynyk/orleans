using Orleans.Concurrency;
using System.Collections.Generic;
using System.Threading.Tasks;
using System;
using Orleans.Runtime;
using System.Reflection;
using System.Linq;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;

namespace Orleans.Indexing
{
    /// <summary>
    /// IndexableGrainNonFaultTolerant class is the super-class of all grains that
    /// need to have indexing capability but without fault-tolerance requirements.
    /// 
    /// To make a grain indexable, two steps should be taken:
    ///     1- the grain class should extend IndexableGrainNonFaultTolerant
    ///     2- the grain class is responsible for calling UpdateIndexes
    ///        whenever one or more indexes need to be updated
    /// </summary>
    public abstract class IndexableGrainNonFaultTolerant<TState, TProperties> : Grain<TState>, IIndexableGrain<TProperties> where TProperties : new() where TState : new()
    {
        /// <summary>
        /// Whether an update should apply exclusively to unique or non-unique indexes.
        /// </summary>
        protected enum UpdateIndexType
        {
            Unique,
            NonUnique
        }

        /// <summary>
        /// An immutable cached version of IndexInfo (containing IIndexUpdateGenerator) instances for the current indexes on the grain.
        /// </summary>
        private NamedIndexMap _grainIndexes;

        /// <summary>
        /// This flag defines whether there is any unique index defined for this indexable grain
        /// </summary>
        protected bool _isThereAnyUniqueIndex;

        /// <summary>
        /// An immutable copy of before-images of the indexed fields
        /// </summary>
        protected Immutable<IDictionary<string, object>> _beforeImages;

        /// <summary>
        /// A cached grain interface type, which is cached on the first call to getIGrainType()
        /// </summary>
        protected IList<Type> _iGrainTypes = null;

        protected virtual TProperties Properties => DefaultCreatePropertiesFromState();
        protected TProperties _props;

        // IndexManager (and therefore logger) cannot be set in ctor because Grain activation has not yet set base.Runtime.
        internal SiloIndexManager SiloIndexManager => IndexManager.GetSiloIndexManager(ref __siloIndexManager, base.ServiceProvider);
        private SiloIndexManager __siloIndexManager;

        private ILogger Logger => __logger ?? (__logger = this.SiloIndexManager.LoggerFactory.CreateLoggerWithFullCategoryName<IndexableGrainNonFaultTolerant<TState, TProperties>>());
        private ILogger __logger;

        private TProperties DefaultCreatePropertiesFromState()
        {
            if (typeof(TProperties).IsAssignableFrom(typeof(TState)))
            {
                return (TProperties)(object)this.State;
            }

            // Copy named property values from this.State to _props. The set of property names will not change.
            if (this._props == null)
            {
                this._props = new TProperties();
            }
            foreach (PropertyInfo p in typeof(TProperties).GetProperties())
            {
                p.SetValue(this._props, typeof(TState).GetProperty(p.Name).GetValue(this.State));
            }
            return this._props;
        }

        // A cache for the work-flow queues, one for each grain interface type that the current IndexableGrain implements
        internal virtual IDictionary<Type, IIndexWorkflowQueue> WorkflowQueues { get; set; }

        /// <summary>
        /// Upon activation, the list of index update generators is retrieved from the index handler. It is cached in
        /// this grain for use in creating before-images, and also for later calls to UpdateIndexes.
        /// 
        /// Then, the before-images are created and stored in memory.
        /// </summary>
        public override Task OnActivateAsync()
        {
            this.Logger.Trace($"Activating indexable grain {Orleans.GrainExtensions.GetGrainId(this)} of type {this.GetIIndexableGrainTypes()[0]} in silo {this.SiloIndexManager.SiloAddress}.");

            // Load indexes
            this._grainIndexes = this.SiloIndexManager.IndexFactory.GetGrainIndexes(GetIIndexableGrainTypes()[0]);
            this._isThereAnyUniqueIndex = this._grainIndexes.HasAnyUniqueIndex;

            // Initialize before images
            this._beforeImages = new Dictionary<string, object>().AsImmutable<IDictionary<string, object>>();
            AddMissingBeforeImages();

            // Insert the current grain to the active indexes defined on this grain and at the same time call OnActivateAsync of the base class
            return Task.WhenAll(InsertIntoActiveIndexes(), base.OnActivateAsync());
        }

        public override Task OnDeactivateAsync()
        {
            this.Logger.Trace($"Deactivating indexable grain {Orleans.GrainExtensions.GetGrainId(this)} of type {this.GetIIndexableGrainTypes()[0]} in silo {this.SiloIndexManager.SiloAddress}.");
            return Task.WhenAll(RemoveFromActiveIndexes(), base.OnDeactivateAsync());
        }

        /// <summary>
        /// Inserts the current grain to the active indexes only if it already has a persisted state
        /// </summary>
        protected Task InsertIntoActiveIndexes()
        {
            // Check if it contains anything to be indexed
            return (this._beforeImages.Value.Values.Any(e => e != null))
                ? UpdateIndexes(this.Properties,
                                     isOnActivate: true,
                                     onlyUpdateActiveIndexes: true,
                                     writeStateIfConstraintsAreNotViolated: false)
                : Task.CompletedTask;
        }

        /// <summary>
        /// Removes the current grain from active indexes
        /// </summary>
        protected Task RemoveFromActiveIndexes()
        {
            // Check if it has anything indexed
            return (this._beforeImages.Value.Values.Any(e => e != null))
                ? UpdateIndexes(default(TProperties),
                                     isOnActivate: false,
                                     onlyUpdateActiveIndexes: true,
                                     writeStateIfConstraintsAreNotViolated: false)
                : Task.CompletedTask;
        }

        /// <summary>
        /// After some changes were made to the grain, and the grain is in a consistent state, this method is called to update the 
        /// indexes defined on this grain type.
        /// 
        /// A call to this method first creates the member updates, and then sends them to ApplyIndexUpdates of the index handler.
        /// 
        /// The only reason that this method can receive a negative result from a call to ApplyIndexUpdates is that the list of indexes
        /// might have changed. In this case, it updates the list of member update and tries again. In the case of a positive result
        /// from ApplyIndexUpdates, the list of before-images is replaced by the list of after-images.
        /// </summary>
        /// <param name="indexableProperties">The properties object containing the indexable properties of this grain</param>
        /// <param name="isOnActivate">Determines whether this method is called upon activation of this grain</param>
        /// <param name="onlyUpdateActiveIndexes">whether only active indexes should be updated</param>
        /// <param name="writeStateIfConstraintsAreNotViolated">whether to write back the state to the storage if no constraint is violated</param>
        protected Task UpdateIndexes(TProperties indexableProperties,
                                     bool isOnActivate,
                                     bool onlyUpdateActiveIndexes,
                                     bool writeStateIfConstraintsAreNotViolated)
        {
            // If there are no indexes defined on this grain, then only the grain state
            // should be written back to the storage (if requested, otherwise nothing should be done)
            if (this._grainIndexes.Count == 0)
            {
                return writeStateIfConstraintsAreNotViolated ? WriteBaseStateAsync() : Task.CompletedTask;
            }

            // A flag to determine whether only unique indexes were updated
            bool onlyUniqueIndexesWereUpdated = this._isThereAnyUniqueIndex;

            // Gather the dictionary of indexes to their corresponding updates
            IDictionary<string, IMemberUpdate> updates =
                GenerateMemberUpdates(indexableProperties, isOnActivate, onlyUpdateActiveIndexes,
                out bool updateIndexesEagerly, ref onlyUniqueIndexesWereUpdated, out int numberOfUniqueIndexesUpdated);

            // Apply the updates to the indexes defined on this grain
            return ApplyIndexUpdates(updates, updateIndexesEagerly,
                onlyUniqueIndexesWereUpdated, numberOfUniqueIndexesUpdated, writeStateIfConstraintsAreNotViolated);
        }

        /// <summary>
        /// Applies a set of updates to the indexes defined on the grain
        /// </summary>
        /// <param name="updates">the dictionary of indexes to their corresponding updates</param>
        /// <param name="updateIndexesEagerly">whether indexes should be updated eagerly or lazily</param>
        /// <param name="onlyUniqueIndexesWereUpdated">a flag to determine whether only unique indexes were updated</param>
        /// <param name="numberOfUniqueIndexesUpdated">determine the number of updated unique indexes</param>
        /// <param name="writeStateIfConstraintsAreNotViolated">whether writing back
        ///             the state to the storage should be done if no constraint is violated</param>
        protected virtual async Task ApplyIndexUpdates(IDictionary<string, IMemberUpdate> updates,
                                                       bool updateIndexesEagerly,
                                                       bool onlyUniqueIndexesWereUpdated,
                                                       int numberOfUniqueIndexesUpdated,
                                                       bool writeStateIfConstraintsAreNotViolated)
        {
            // If there is any update to the indexes we go ahead and update the indexes
            if (updates.Count() > 0)
            {
                IList<Type> iGrainTypes = GetIIndexableGrainTypes();
                var thisGrain = this.AsReference<IIndexableGrain>(base.GrainFactory);
                bool isThereAtMostOneUniqueIndex = numberOfUniqueIndexesUpdated <= 1;

                // Apply any unique index updates eagerly.
                if (numberOfUniqueIndexesUpdated > 0)
                {
                    try
                    {
                        // If there is more than one unique index to update, then updates to the unique indexes should be tentative
                        // so they are not visible to readers before making sure that all uniqueness constraints are satisfied.
                        await ApplyIndexUpdatesEagerly(iGrainTypes, thisGrain, updates, UpdateIndexType.Unique, updateIndexesTentatively:!isThereAtMostOneUniqueIndex);
                    }
                    catch (UniquenessConstraintViolatedException ex)
                    {
                        // If any uniqueness constraint is violated and we have more than one unique index defined, then all tentative
                        // updates must be undone, then the exception is thrown back to the user code.
                        if (!isThereAtMostOneUniqueIndex)
                        {
                            await UndoTentativeChangesToUniqueIndexesEagerly(iGrainTypes, thisGrain, updates);
                        }
                        throw ex;
                    }
                }

                if (updateIndexesEagerly)
                {
                    // Case 1: if only unique indexes were updated, then their update is already processed before and
                    // the only thing remaining is to save the grain state if requested
                    if (onlyUniqueIndexesWereUpdated && writeStateIfConstraintsAreNotViolated)
                    {
                        await WriteBaseStateAsync();
                    }

                    // Case 2: if there were some non-unique indexes updates and writing the state back to the storage
                    // is requested, then we do these two tasks concurrently
                    else if (writeStateIfConstraintsAreNotViolated)
                    {
                        await Task.WhenAll(
                            WriteBaseStateAsync(),
                            onlyUniqueIndexesWereUpdated ? Task.CompletedTask : ApplyIndexUpdatesEagerly(iGrainTypes, thisGrain, updates, UpdateIndexType.NonUnique)
                        );
                    }

                    // Case 3: if there were some non-unique indexes updates, but writing the state back to the storage
                    // is not requested, then the only thing left is updating the remaining non-unique indexes
                    else if (!onlyUniqueIndexesWereUpdated)
                    {
                        await ApplyIndexUpdatesEagerly(iGrainTypes, thisGrain, updates, UpdateIndexType.NonUnique);
                    }
                }
                else // !updateIndexesEagerly
                {
                    ApplyIndexUpdatesLazilyWithoutWait(updates, iGrainTypes, thisGrain, Guid.NewGuid());
                    if (writeStateIfConstraintsAreNotViolated)
                    {
                        await WriteBaseStateAsync();
                    }
                }

                // If everything was successful, the before images are updated
                UpdateBeforeImages(updates);
            }

            // Otherwise if there is no update to the indexes, we should write back the state of the grain if requested.
            else if (writeStateIfConstraintsAreNotViolated)
            {
                await WriteBaseStateAsync();
            }
        }

        private Task UndoTentativeChangesToUniqueIndexesEagerly(IList<Type> iGrainTypes,
                                                       IIndexableGrain thisGrain,
                                                       IDictionary<string, IMemberUpdate> updates)
        {
            return ApplyIndexUpdatesEagerly(iGrainTypes, thisGrain, MemberUpdateReverseTentative.Reverse(updates), UpdateIndexType.Unique, updateIndexesTentatively:false);
        }

        /// <summary>
        /// Lazily Applies updates to the indexes defined on this grain
        /// 
        /// The lazy update involves adding a work-flow record to the corresponding IIndexWorkflowQueue for this grain.
        /// </summary>
        /// <param name="updates">the dictionary of updates for each index</param>
        /// <param name="iGrainTypes">the grain interface type implemented by this grain</param>
        /// <param name="thisGrain">the grain reference for the current grain</param>
        /// <param name="workflowID">the workflow identifier</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ApplyIndexUpdatesLazilyWithoutWait(IDictionary<string, IMemberUpdate> updates,
                                             IList<Type> iGrainTypes,
                                             IIndexableGrain thisGrain,
                                             Guid workflowID)
        {
            ApplyIndexUpdatesLazily(updates, iGrainTypes, thisGrain, workflowID).Ignore();
        }

        /// <summary>
        /// Lazily applies updates to the indexes defined on this grain
        /// 
        /// The lazy update involves adding a work-flow record to the corresponding IIndexWorkflowQueue for this grain.
        /// </summary>
        /// <param name="updates">the dictionary of updates for each index</param>
        /// <param name="iGrainTypes">the grain interface type implemented by this grain</param>
        /// <param name="thisGrain">the grain reference for the current grain</param>
        /// <param name="workflowID">the workflow identifier</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected Task ApplyIndexUpdatesLazily(IDictionary<string, IMemberUpdate> updates,
                                             IList<Type> iGrainTypes,
                                             IIndexableGrain thisGrain,
                                             Guid workflowID)
        {
            Task addToQueue(Type iGrainType) => GetWorkflowQueue(iGrainType).AddToQueue(new IndexWorkflowRecord(workflowID, thisGrain, updates).AsImmutable());
            return iGrainTypes.Count() == 1
                ? addToQueue(iGrainTypes[0])
                : Task.WhenAll(iGrainTypes.Select(iGrainType => addToQueue(iGrainType)));
        }

        /// <summary>
        /// Eagerly Applies updates to the indexes defined on this grain
        /// </summary>
        /// <param name="iGrainTypes">the list of grain interface types implemented by this grain</param>
        /// <param name="updatedGrain">the grain reference for the current updated grain</param>
        /// <param name="updates">the dictionary of updates for each index</param>
        /// <param name="updateIndexType">indicates whether only unique or only non-unique indexes should be updated</param>
        /// <param name="updateIndexesTentatively">a flag to determine whether
        /// updates to indexes should be tentatively done. That is, the update
        /// won't be visible to readers, but prevents writers from overwriting
        /// them an violating constraints</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected async Task ApplyIndexUpdatesEagerly(IList<Type> iGrainTypes,
                                                    IIndexableGrain updatedGrain,
                                                    IDictionary<string, IMemberUpdate> updates,
                                                    UpdateIndexType updateIndexType,
                                                    bool updateIndexesTentatively = false)
        {
            Task applyUpdate(Type iGrainType) => ApplyIndexUpdatesEagerly(iGrainType, updatedGrain, updates, updateIndexType, updateIndexesTentatively);
            await (iGrainTypes.Count() == 1
                ? applyUpdate(iGrainTypes[0])
                : Task.WhenAll(iGrainTypes.Select(iGrainType => applyUpdate(iGrainType))));
        }

        /// <summary>
        /// Eagerly Applies updates to the indexes defined on this grain for a single grain interface type implemented by this grain
        /// </summary>
        /// <param name="iGrainType">a single grain interface type implemented by this grain</param>
        /// <param name="updatedGrain">the grain reference for the current updated grain</param>
        /// <param name="updates">the dictionary of updates for each index</param>
        /// <param name="updateIndexType">indicates whether only unique or only non-unique indexes should be updated</param>
        /// <param name="updateIndexesTentatively">a flag to determine whether updates to indexes should be tentatively done. That is, the update
        ///             won't be visible to readers, but prevents writers from overwriting them an violating constraints</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Task ApplyIndexUpdatesEagerly(Type iGrainType,
                                              IIndexableGrain updatedGrain,
                                              IDictionary<string, IMemberUpdate> updates,
                                              UpdateIndexType updateIndexType,
                                              bool updateIndexesTentatively)
        {
            var wantOnlyUniqueIndex = updateIndexType == UpdateIndexType.Unique;
            IList<Task<bool>> updateIndexTasks = new List<Task<bool>>();
            foreach (KeyValuePair<string, IMemberUpdate> updt in updates.Where(updt => updt.Value.GetOperationType() != IndexOperationType.None))
            {
                var idxInfo = this._grainIndexes[updt.Key];
                var isUniqueIndex = idxInfo.MetaData.IsUniqueIndex;

                // The actual update happens if either the corresponding index is not a unique index
                // and the caller asks for only updating non-unique indexes, or the corresponding
                // index is a unique index and the caller asks for only updating unique indexes.
                if (wantOnlyUniqueIndex == isUniqueIndex)
                {
                    // If the caller asks for the update to be tentative, then it will be wrapped inside a MemberUpdateTentative
                    IMemberUpdate updateToIndex = updateIndexesTentatively ? new MemberUpdateTentative(updt.Value) : updt.Value;

                    // The update task is added to the list of update tasks
                    updateIndexTasks.Add(idxInfo.IndexInterface.ApplyIndexUpdate(this.SiloIndexManager.RuntimeClient,
                                            updatedGrain, updateToIndex.AsImmutable(), isUniqueIndex, idxInfo.MetaData, base.SiloAddress));
                }
            }

            // At the end, because the index update should be eager, we wait for all index update tasks to finish
            return Task.WhenAll(updateIndexTasks);
        }

        /// <summary>
        /// Generates the member updates based on the index update generator configured for the grain.
        /// </summary>
        /// <param name="indexableProperties">The properties object containing the indexable properties of this grain</param>
        /// <param name="isOnActivate">Determines whether this method is called upon activation of this grain</param>
        /// <param name="onlyUpdateActiveIndexes">whether only active indexes should be updated</param>
        /// <param name="updateIndexesEagerly">a flag to determine whether indexes should be updated eagerly (as opposed to being updated lazily)</param>
        /// <param name="onlyUniqueIndexesWereUpdated">a flag to determine whether only unique indexes were updated</param>
        /// <param name="numberOfUniqueIndexesUpdated">determine the number of updated unique indexes</param>
        /// <returns>a dictionary of index name mapped to the update information</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private IDictionary<string, IMemberUpdate> GenerateMemberUpdates(TProperties indexableProperties, bool isOnActivate, bool onlyUpdateActiveIndexes, out bool updateIndexesEagerly,
                                                                         ref bool onlyUniqueIndexesWereUpdated, out int numberOfUniqueIndexesUpdated)
        {
            KeyValuePair<string, IndexInfo>? kvpFirstIndex = null;
            numberOfUniqueIndexesUpdated = 0;

            IDictionary<string, IMemberUpdate> updates = new Dictionary<string, IMemberUpdate>();
            {
                IDictionary<string, object> befImgs = this._beforeImages.Value;
                foreach (var kvp in this._grainIndexes)
                {
                    var idxInfo = kvp.Value;
                    if (!onlyUpdateActiveIndexes || !(idxInfo.IndexInterface is ITotalIndex))
                    {
                        IMemberUpdate mu = isOnActivate ? idxInfo.UpdateGenerator.CreateMemberUpdate(befImgs[kvp.Key])
                                                        : idxInfo.UpdateGenerator.CreateMemberUpdate(indexableProperties, befImgs[kvp.Key]);
                        if (mu.GetOperationType() != IndexOperationType.None)
                        {
                            updates.Add(kvp.Key, mu);
                            var indexMetaData = kvp.Value.MetaData;

                            // This flag must be the same for all indexes defined on a grain.
                            if (!kvpFirstIndex.HasValue)
                            {
                                kvpFirstIndex = kvp;
                            }
#if false // TODO: inconsistent eagerness
                            else if (kvpFirstIndex.Value.Value.MetaData.IsEager != indexMetaData.IsEager)
                            {
                                throw new InvalidOperationException($"Inconsistent index eagerness specification on grain implementation {this.GetType().FullName} properties {typeof(TProperties).FullName}." +
                                                                    $" Index {kvpFirstIndex.Value.Key} specified {kvpFirstIndex.Value.Value.MetaData.IsEager} while" +
                                                                    $" index {kvp.Key} specified {kvp.Value.MetaData.IsEager}. This misconfiguration should have been detected on silo startup.");
                            }
#endif

                            // Update "unique index"-related output flags and counters
                            bool isUniqueIndex = indexMetaData.IsUniqueIndex;
                            onlyUniqueIndexesWereUpdated = onlyUniqueIndexesWereUpdated && isUniqueIndex;
                            if (isUniqueIndex) ++numberOfUniqueIndexesUpdated;
                        }
                    }
                }
            }

            updateIndexesEagerly = kvpFirstIndex.HasValue ? kvpFirstIndex.Value.Value.MetaData.IsEager : false;
            return updates;
        }

        /// <summary>
        /// This method finds the IGrain interface that is the lowest one in the interface type hierarchy of the current grain
        /// </summary>
        /// <returns>lowest IGrain interface in the hierarchy that the current class implements</returns>
        protected IList<Type> GetIIndexableGrainTypes()
        {
            if (this._iGrainTypes == null)
            {
                this._iGrainTypes = new List<Type>();
                Type iIndexableGrainTp = typeof(IIndexableGrain<TProperties>);

                Type[] interfaces = GetType().GetInterfaces();
                int numInterfaces = interfaces.Length;

                for (int i = 0; i < numInterfaces; ++i)
                {
                    Type otherIGrainType = interfaces[i];

                    //iIndexableGrainTp and typedIIndexableGrainTp are ignored when checking the descendants of IGrain,
                    // because there is no guarantee user defined grain interfaces extend these interfaces
                    if (iIndexableGrainTp != otherIGrainType && iIndexableGrainTp.IsAssignableFrom(otherIGrainType))
                    {
                        this._iGrainTypes.Add(otherIGrainType);
                    }
                }
            }
            return this._iGrainTypes;
        }

        /// <summary>
        /// This method checks the list of cached indexes, and if any index does not have a before-image, it will create
        /// one for it. As before-images are stored as an immutable field, a new map is created in this process.
        /// 
        /// This method is called on activation of the grain, and when the UpdateIndexes method detects an inconsistency
        /// between the indexes in the index handler and the cached indexes of the current grain.
        /// </summary>
        private void AddMissingBeforeImages()
        {
            IDictionary<string, object> oldBefImgs = this._beforeImages.Value;
            IDictionary<string, object> newBefImgs = new Dictionary<string, object>();
            foreach (var idxOp in this._grainIndexes)
            {
                var indexID = idxOp.Key;
                var oldBefImg = oldBefImgs.ContainsKey(indexID)
                    ? oldBefImgs[indexID]
                    : idxOp.Value.UpdateGenerator.ExtractIndexImage(this.Properties);
                newBefImgs.Add(indexID, oldBefImg);
            }
            this._beforeImages = newBefImgs.AsImmutable();
        }

        /// <summary>
        /// This method assumes that a set of changes is applied to the indexes, and then it replaces the current before-images
        /// with after-images produced by the update.
        /// </summary>
        /// <param name="updates">the member updates that were successfully applied to the current indexes</param>
        protected void UpdateBeforeImages(IDictionary<string, IMemberUpdate> updates)
        {
            IDictionary<string, object> befImgs = new Dictionary<string, object>(this._beforeImages.Value);
            foreach (KeyValuePair<string, IMemberUpdate> updt in updates)
            {
                var indexID = updt.Key;
                var opType = updt.Value.GetOperationType();
                if (opType == IndexOperationType.Update || opType == IndexOperationType.Insert)
                {
                    befImgs[indexID] = _grainIndexes[indexID].UpdateGenerator.ExtractIndexImage(this.Properties);
                }
                else if (opType == IndexOperationType.Delete)
                {
                    befImgs[indexID] = null;
                }
            }
            this._beforeImages = befImgs.AsImmutable();
        }

        protected override async Task WriteStateAsync()
        {
            // WriteBaseStateAsync should be done before UpdateIndexes, in order to ensure that only the successfully persisted bits get to be indexed,
            // so we cannot do these two tasks in parallel
            //await Task.WhenAll(WriteBaseStateAsync(), UpdateIndexes());

            // During WriteStateAsync for a stateful indexable grain, the indexes get updated concurrently while WriteBaseStateAsync is done.
            await UpdateIndexes(this.Properties, isOnActivate: false, onlyUpdateActiveIndexes: false, writeStateIfConstraintsAreNotViolated: true);
        }

        /// <summary>
        /// Writes the state of the grain back to the storage
        /// without updating the indexes
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected Task WriteBaseStateAsync()
            => base.WriteStateAsync();

        Task<object> IIndexableGrain.ExtractIndexImage(IIndexUpdateGenerator iUpdateGen)
            => Task.FromResult(iUpdateGen.ExtractIndexImage(this.Properties));

        public virtual Task<Immutable<HashSet<Guid>>> GetActiveWorkflowIdsList()
            => throw new NotSupportedException();

        public virtual Task RemoveFromActiveWorkflowIds(HashSet<Guid> removedWorkflowId)
            => throw new NotSupportedException();

        /// <summary>
        /// Find the corresponding work-flow queue for a given grain interface
        /// type that the current IndexableGrain implements
        /// </summary>
        /// <param name="iGrainType">the given grain interface type</param>
        /// <returns>the work-flow queue corresponding to the iGrainType</returns>
        internal IIndexWorkflowQueue GetWorkflowQueue(Type iGrainType)
        {
            if (this.WorkflowQueues == null)
            {
                this.WorkflowQueues = new Dictionary<Type, IIndexWorkflowQueue>();
            }

            if (!this.WorkflowQueues.TryGetValue(iGrainType, out IIndexWorkflowQueue workflowQ))
            {
                workflowQ = IndexWorkflowQueueBase.GetIndexWorkflowQueueFromGrainHashCode(this.SiloIndexManager, iGrainType,
                        this.AsReference<IIndexableGrain>(this.GrainFactory, iGrainType).GetHashCode(), base.SiloAddress);
                this.WorkflowQueues.Add(iGrainType, workflowQ);
            }
            return workflowQ;
        }
    }

    /// <summary>
    /// This stateless IndexableGrainNonFaultTolerant is the super class of all stateless 
    /// indexable-grains. But as multiple-inheritance (from both <see cref="Grain{T}"/> and 
    /// <see cref="IndexableGrainNonFaultTolerant{T}"/>) is not allowed, this class extends
    /// IndexableGrainNonFaultTolerant{object} and disables the storage functionality of Grain{T}.
    /// </summary>
    public abstract class IndexableGrainNonFaultTolerant<TProperties> : IndexableGrainNonFaultTolerant<object, TProperties>, IIndexableGrain<TProperties> where TProperties : new()
    {
        protected override Task ClearStateAsync() => Task.CompletedTask;

        protected override Task WriteStateAsync()
        {
            // The only thing that should be done during WriteStateAsync for a stateless indexable grain is to update its indexes
            return UpdateIndexes(this.Properties, isOnActivate: false, onlyUpdateActiveIndexes: false, writeStateIfConstraintsAreNotViolated: false);
        }

        protected override Task ReadStateAsync() => Task.CompletedTask;
    }
}
