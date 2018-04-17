using System;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Orleans.ApplicationParts;
using Orleans.Runtime;

namespace Orleans.Indexing
{
    internal class ApplicationPartsIndexableGrainLoader
    {
        private readonly IndexManager indexManager;
        private readonly SiloIndexManager siloIndexManager;
        private readonly ILogger logger;

        private readonly Type indexAttrType = typeof(IndexAttribute);
        private readonly PropertyInfo indexTypeProperty = typeof(IndexAttribute).GetProperty(nameof(IndexAttribute.IndexType));
        private readonly PropertyInfo isEagerProperty = typeof(IndexAttribute).GetProperty(nameof(IndexAttribute.IsEager));
        private readonly PropertyInfo isUniqueProperty = typeof(IndexAttribute).GetProperty(nameof(IndexAttribute.IsUnique));
        private readonly PropertyInfo maxEntriesPerBucketProperty = typeof(IndexAttribute).GetProperty(nameof(IndexAttribute.MaxEntriesPerBucket));

        private bool IsInSilo => this.siloIndexManager != null;

        internal ApplicationPartsIndexableGrainLoader(IndexManager indexManager)
        {
            this.indexManager = indexManager;
            this.siloIndexManager = indexManager as SiloIndexManager;
            this.logger = this.indexManager.LoggerFactory.CreateLoggerWithFullCategoryName<ApplicationPartsIndexableGrainLoader>();
        }

        /// <summary>
        /// This method crawls the assemblies and looks for the index definitions (determined by extending the IIndexableGrain{TProperties}
        /// interface and adding annotations to properties in TProperties).
        /// </summary>
        /// <returns>An index registry for the silo. </returns>
        public IndexRegistry GetGrainClassIndexes()
        {
            Type[] grainTypes = this.indexManager.ApplicationPartManager.ApplicationParts.OfType<AssemblyPart>()
                                    .SelectMany(part => TypeUtils.GetTypes(part.Assembly, TypeUtils.IsConcreteGrainClass, this.logger))
                                    .ToArray();

            var registry = new IndexRegistry();
            foreach (var grainType in grainTypes)
            {
                if (registry.ContainsKey(grainType))
                {
                    throw new InvalidOperationException($"Precondition violated: GetGrainClassIndexes should not encounter a duplicate type ({TypeUtils.GetFullName(grainType)})");
                }
                GetIndexesForASingleGrainType(registry, grainType);
            }
            return registry;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void GetIndexesForASingleGrainType(IndexRegistry registry, Type grainType)
        {
            Type[] interfaces = grainType.GetInterfaces();
            int numInterfaces = interfaces.Length;

            //iterate over the interfaces of the grain type
            for (int i = 0; i < numInterfaces; ++i)
            {
                Type iIndexableGrain = interfaces[i];

                // If the interface extends IIndexableGrain<TProperties> interface...
                if (iIndexableGrain.IsGenericType && iIndexableGrain.GetGenericTypeDefinition() == typeof(IIndexableGrain<>))
                {
                    Type propertiesArg = iIndexableGrain.GetGenericArguments()[0];
                    // ... and if TProperties is a class... 
                    if (propertiesArg.GetTypeInfo().IsClass)
                    {
                        // ... then the indexes are added to all the descendant interfaces of IIndexableGrain<TProperties>;
                        // these interfaces are defined by end-users.
                        for (int j = 0; j < numInterfaces; ++j)
                        {
                            Type userDefinedIGrain = interfaces[j];
                            CreateIndexesForASingleInterfaceOfAGrainType(registry, iIndexableGrain, propertiesArg, userDefinedIGrain, grainType);
                        }
                    }
                    break;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CreateIndexesForASingleInterfaceOfAGrainType(IndexRegistry registry, Type iIndexableGrain, Type propertiesArg, Type userDefinedIGrain, Type userDefinedGrainImpl)
        {
            // If the given interface is a user-defined interface extending IIndexableGrain<TProperties>
            if (iIndexableGrain != userDefinedIGrain && iIndexableGrain.IsAssignableFrom(userDefinedIGrain) && !registry.ContainsKey(userDefinedIGrain))
            {
                // Check either:
                // - all indexes are defined as lazy, -or-
                // - all indexes are defined as lazy and none of them are Total Index (because Total Indexes cannot be lazy)
                CheckAllIndexesAreEitherLazyOrEager(propertiesArg, userDefinedIGrain, userDefinedGrainImpl);

                // All the properties in TProperties are scanned for Index annotation.
                // If found, the index is created using the information provided in the annotation.
                NamedIndexMap indexesOnGrain = new NamedIndexMap();
                var hasNonEagerIndex = false;
                foreach (PropertyInfo p in propertiesArg.GetProperties())
                {
                    var indexAttrs = p.GetCustomAttributes(typeof(IndexAttribute), false);
                    foreach (var indexAttr in indexAttrs)
                    {
                        string indexName = "__" + p.Name;
                        if (indexesOnGrain.ContainsKey(indexName))
                        {
                            throw new InvalidOperationException($"An index named {indexName} already exists for user-defined grain interface {userDefinedIGrain.Name}");
                        }

                        Type indexType = (Type)this.indexTypeProperty.GetValue(indexAttr);
                        if (indexType.IsGenericTypeDefinition)
                        {
                            indexType = indexType.MakeGenericType(p.PropertyType, userDefinedIGrain);
                        }

                        // If it's not eager, then it's configured to be lazily updated
                        bool isEager = (bool)isEagerProperty.GetValue(indexAttr);
                        if (!isEager) hasNonEagerIndex = true;
                        bool isUnique = (bool)isUniqueProperty.GetValue(indexAttr);
                        int maxEntriesPerBucket = (int)maxEntriesPerBucketProperty.GetValue(indexAttr);
                        indexesOnGrain[indexName] = this.indexManager.IndexFactory.CreateIndex(indexType, indexName, isUnique, isEager, maxEntriesPerBucket, p);
                        this.logger.Info($"Index created: Interface = {userDefinedIGrain.Name}, property = {propertiesArg.Name}, index = {indexName}");
                    }
                }
                registry[userDefinedIGrain] = indexesOnGrain;
                if (this.IsInSilo && hasNonEagerIndex)
                {
                    IndexFactory.RegisterIndexWorkflowQueues(this.siloIndexManager, userDefinedIGrain, userDefinedGrainImpl);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckAllIndexesAreEitherLazyOrEager(Type propertiesArg, Type userDefinedIGrain, Type userDefinedGrainImpl)
        {
            bool isFaultTolerant = IsSubclassOfRawGenericType(typeof(IndexableGrain<,>), userDefinedGrainImpl);
            foreach (PropertyInfo p in propertiesArg.GetProperties())
            {
                var indexAttrs = p.GetCustomAttributes(this.indexAttrType, false);
                var isFirstIndexEager = (indexAttrs.Length > 0) ? (bool)isEagerProperty.GetValue(indexAttrs[0]) : false;
                foreach (var indexAttr in indexAttrs)
                {
                    bool isEager = (bool)isEagerProperty.GetValue(indexAttr);
                    Type indexType = (Type)indexTypeProperty.GetValue(indexAttr);
                    bool isTotalIndex = typeof(ITotalIndex).IsAssignableFrom(indexType);

                    //Total Index cannot be configured as being lazy
                    if (isTotalIndex && isEager)
                    {
                        throw new InvalidOperationException(string.Format("A Total Index cannot be configured to be updated eagerly. The only option for updating a Total Index is lazy updating. Total Index of type {0} is defined to be updated eagerly on property {1} of class {2} on {3} grain interface.", TypeUtils.GetFullName(indexType), p.Name, TypeUtils.GetFullName(propertiesArg), TypeUtils.GetFullName(userDefinedIGrain)));
                    }
                    else if (isFaultTolerant && isEager)
                    {
                        throw new InvalidOperationException(string.Format("A fault-tolerant grain implementation cannot be configured to eagerly update its indexes. The only option for updating the indexes of a fault-tolerant indexable grain is lazy updating. The index of type {0} is defined to be updated eagerly on property {1} of class {2} on {3} grain implementation class.", TypeUtils.GetFullName(indexType), p.Name, TypeUtils.GetFullName(propertiesArg), TypeUtils.GetFullName(userDefinedGrainImpl)));
                    }
                    else if (isEager != isFirstIndexEager)
                    {
                        throw new InvalidOperationException(string.Format("Some indexes on property class {0} of {1} grain interface are defined to be updated eagerly while others are configured as lazy updating. You should fix this by configuring all indexes to be updated lazily or eagerly. If you have at least one Total Index among your indexes, then all other indexes should be configured as lazy, too.", TypeUtils.GetFullName(propertiesArg), TypeUtils.GetFullName(userDefinedIGrain)));
                    }
                }
            }
        }

        public static bool IsSubclassOfRawGenericType(Type genericType, Type typeToCheck)
        {
            for (; typeToCheck != null && typeToCheck != typeof(object); typeToCheck = typeToCheck.BaseType)
            {
                if (genericType == (typeToCheck.IsGenericType ? typeToCheck.GetGenericTypeDefinition() : typeToCheck))
                {
                    return true;
                }
            }
            return false;
        }
    }
}
