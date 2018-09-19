using System;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
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

        private static readonly Type indexAttrType = typeof(IndexAttribute);
        private static readonly PropertyInfo indexTypeProperty = typeof(IndexAttribute).GetProperty(nameof(IndexAttribute.IndexType));
        private static readonly PropertyInfo isEagerProperty = typeof(IndexAttribute).GetProperty(nameof(IndexAttribute.IsEager));
        private static readonly PropertyInfo isUniqueProperty = typeof(IndexAttribute).GetProperty(nameof(IndexAttribute.IsUnique));
        private static readonly PropertyInfo maxEntriesPerBucketProperty = typeof(IndexAttribute).GetProperty(nameof(IndexAttribute.MaxEntriesPerBucket));

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
        public async Task<IndexRegistry> GetGrainClassIndexes()
        {
            Type[] grainTypes = this.indexManager.ApplicationPartManager.ApplicationParts.OfType<AssemblyPart>()
                                    .SelectMany(part => part.Assembly.GetConcreteGrainClasses(this.logger))
                                    .ToArray();
            return await GetIndexRegistry(this, grainTypes);
        }

        internal async static Task<IndexRegistry> GetIndexRegistry(ApplicationPartsIndexableGrainLoader loader, Type[] grainTypes)
        {
            var registry = new IndexRegistry();
            foreach (var grainType in grainTypes)
            {
                if (registry.ContainsKey(grainType))
                {
                    throw new InvalidOperationException($"Precondition violated: GetGrainClassIndexes should not encounter a duplicate type ({IndexUtils.GetFullTypeName(grainType)})");
                }
                await GetIndexesForASingleGrainType(loader, registry, grainType);
            }
            return registry;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private async static Task GetIndexesForASingleGrainType(ApplicationPartsIndexableGrainLoader loader, IndexRegistry registry, Type grainType)
        {
            Type[] interfaces = grainType.GetInterfaces();
            bool? grainIndexesAreEager = null;

            // If there is an interface that directly extends IIndexableGrain<TProperties>...
            Type iIndexableGrain = interfaces.Where(itf => itf.IsGenericType && itf.GetGenericTypeDefinition() == typeof(IIndexableGrain<>)).FirstOrDefault();
            if (iIndexableGrain != null)
            {
                // ... and its generic argument is a class (TProperties)... 
                Type propertiesArgType = iIndexableGrain.GetGenericArguments()[0];
                if (propertiesArgType.GetTypeInfo().IsClass)
                {
                    // ... then add the indexes for all the descendant interfaces of IIndexableGrain<TProperties>; these interfaces are defined by end-users.
                    foreach (Type userDefinedIGrain in interfaces.Where(itf => iIndexableGrain != itf && iIndexableGrain.IsAssignableFrom(itf) && !registry.ContainsKey(itf)))
                    {
                        grainIndexesAreEager = await CreateIndexesForASingleInterface(loader, registry, propertiesArgType, userDefinedIGrain, grainType, grainIndexesAreEager);
                    }
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private async static Task<bool?> CreateIndexesForASingleInterface(ApplicationPartsIndexableGrainLoader loader, IndexRegistry registry, Type propertiesArgType,
                                                                                      Type userDefinedIGrain, Type userDefinedGrainImpl, bool? grainIndexesAreEager)
        {
            // All the properties in TProperties are scanned for Index annotation.
            // If found, the index is created using the information provided in the annotation.
            NamedIndexMap indexesOnGrain = new NamedIndexMap();
            var interfaceHasLazyIndex = false;  // Use a separate value from grainIndexesAreEager in case we change to allow mixing eager and lazy on a single grain.
            foreach (PropertyInfo propInfo in propertiesArgType.GetProperties())
            {
                var indexAttrs = propInfo.GetCustomAttributes<IndexAttribute>(inherit:false);
                foreach (var indexAttr in indexAttrs)
                {
                    string indexName = "__" + propInfo.Name;
                    if (indexesOnGrain.ContainsKey(indexName))
                    {
                        throw new InvalidOperationException($"An index named {indexName} already exists for user-defined grain interface {userDefinedIGrain.Name}");
                    }

                    Type indexType = (Type)indexTypeProperty.GetValue(indexAttr);
                    if (indexType.IsGenericTypeDefinition)
                    {
                        indexType = indexType.MakeGenericType(propInfo.PropertyType, userDefinedIGrain);
                    }

                    // If it's not eager, then it's configured to be lazily updated
                    bool isEager = (bool)isEagerProperty.GetValue(indexAttr);
                    if (!isEager) interfaceHasLazyIndex = true;
                    if (!grainIndexesAreEager.HasValue) grainIndexesAreEager = isEager;
                    bool isUnique = (bool)isUniqueProperty.GetValue(indexAttr);

                    ValidateSingleIndex(indexAttr, userDefinedIGrain, userDefinedGrainImpl, propertiesArgType, propInfo, grainIndexesAreEager, isEager, isUnique);

                    int maxEntriesPerBucket = (int)maxEntriesPerBucketProperty.GetValue(indexAttr);
                    if (loader != null)
                    {
                        await loader.CreateIndex(propertiesArgType, userDefinedIGrain, indexesOnGrain, propInfo, indexName, indexType, isEager, isUnique, maxEntriesPerBucket);
                    }
                }
            }
            registry[userDefinedIGrain] = indexesOnGrain;
            if (interfaceHasLazyIndex && loader != null)
            {
                await loader.RegisterWorkflowQueues(userDefinedIGrain, userDefinedGrainImpl);
            }
            return grainIndexesAreEager;
        }

        private async Task CreateIndex(Type propertiesArg, Type userDefinedIGrain, NamedIndexMap indexesOnGrain, PropertyInfo property,
                                       string indexName, Type indexType, bool isEager, bool isUnique, int maxEntriesPerBucket)
        {
            indexesOnGrain[indexName] = await this.indexManager.IndexFactory.CreateIndex(indexType, indexName, isUnique, isEager, maxEntriesPerBucket, property);
            this.logger.Info($"Index created: Interface = {userDefinedIGrain.Name}, property = {propertiesArg.Name}, index = {indexName}");
        }

        private async Task RegisterWorkflowQueues(Type userDefinedIGrain, Type userDefinedGrainImpl)
        {
            if (this.IsInSilo)
            {
                await IndexFactory.RegisterIndexWorkflowQueues(this.siloIndexManager, userDefinedIGrain, userDefinedGrainImpl);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void ValidateSingleIndex(IndexAttribute indexAttr, Type userDefinedIGrain, Type userDefinedGrainImpl, Type propertiesArgType, PropertyInfo propInfo,
                                                bool? grainIndexesAreEager, bool isEager, bool isUnique)
        {
            bool isFaultTolerant = IsSubclassOfRawGenericType(typeof(IndexableGrain<,>), userDefinedGrainImpl);
            Type indexType = (Type)indexTypeProperty.GetValue(indexAttr);
            bool isTotalIndex = typeof(ITotalIndex).IsAssignableFrom(indexType);

            if (indexAttr is ActiveIndexAttribute && isUnique)
            {
                // See comments in ActiveIndexAttribute for details of why this is disallowed.
                throw new InvalidOperationException($"An active Index cannot be configured to be unique, because multiple activations, persisting, and deactivations can create duplicates." +
                                                    $" Active Index of type {IndexUtils.GetFullTypeName(indexType)} is defined to be unique on property {propInfo.Name}" +
                                                    $" of class {IndexUtils.GetFullTypeName(propertiesArgType)} on {IndexUtils.GetFullTypeName(userDefinedIGrain)} grain interface.");
            }
            if (isTotalIndex && isEager)
            {
                throw new InvalidOperationException($"A Total Index cannot be configured to be updated eagerly. The only option for updating a Total Index is lazy updating." +
                                                    $" Total Index of type {IndexUtils.GetFullTypeName(indexType)} is defined to be updated eagerly on property {propInfo.Name}" +
                                                    $" of class {IndexUtils.GetFullTypeName(propertiesArgType)} on {IndexUtils.GetFullTypeName(userDefinedIGrain)} grain interface.");
            }
            if (isFaultTolerant && isEager)
            {
                throw new InvalidOperationException($"A fault-tolerant grain implementation cannot be configured to eagerly update its indexes." +
                                                    $" The only option for updating the indexes of a fault-tolerant indexable grain is lazy updating." +
                                                    $" The index of type {IndexUtils.GetFullTypeName(indexType)} is defined to be updated eagerly on property {propInfo.Name}" +
                                                    $" of class {IndexUtils.GetFullTypeName(propertiesArgType)} on {IndexUtils.GetFullTypeName(userDefinedGrainImpl)} grain implementation class.");
            }
            if (grainIndexesAreEager.HasValue && grainIndexesAreEager.Value != isEager)
            {
                throw new InvalidOperationException($"Some indexes on {IndexUtils.GetFullTypeName(userDefinedGrainImpl)} grain implementation class are defined as eager while others are lazy." +
                                                    $" The index of type {IndexUtils.GetFullTypeName(indexType)} is defined to be updated {(isEager ? "eagerly" : "lazily")} on property { propInfo.Name}" +
                                                    $" of property class {IndexUtils.GetFullTypeName(propertiesArgType)} on {IndexUtils.GetFullTypeName(userDefinedIGrain)} grain interface," +
                                                    $" while previous indexs have been configured to be updated {(isEager ? "lazily" : "eagerly")}." +
                                                    $" You must fix this by configuring all indexes to be updated lazily or eagerly." +
                                                    $" Note: If you have at least one Total Index among your indexes, this must be lazy, and thus all other indexes must be lazy also.");
            }
            if (!VerifyNullValue(propInfo, isUnique, out string convertErrMsg))
            {
                throw new InvalidOperationException($"The index of type {IndexUtils.GetFullTypeName(indexType)} on {IndexUtils.GetFullTypeName(userDefinedGrainImpl)} grain implementation class" +
                                                    $" failed verification. " + convertErrMsg);
            }
        }

        internal static bool VerifyNullValue(PropertyInfo propInfo, bool isUnique, out string errorMessage)
        {
            errorMessage = string.Empty;
            var indexAttrs = propInfo.GetCustomAttributes<IndexAttribute>(inherit: false).Where(attr => !string.IsNullOrEmpty(attr.NullValue)).ToArray();

            if (propInfo.PropertyType.IsNullable())
            {
                if (indexAttrs.Length > 0)
                {
                    errorMessage = $"Cannot specify a NullValue attribute for nullable property {propInfo.Name}";
                    return false;
                }
                return true;
            }

            if (isUnique && indexAttrs.Length == 0)
            {
                errorMessage = $"Must specify a NullValue attribute for non-nullable property {propInfo.Name} that participates in a unique index";
                return false;
            }

            string firstValue = null;
            foreach (var attr in indexAttrs)
            {
                if (firstValue == null)
                {
                    firstValue = attr.NullValue;
                }
                else if (firstValue != attr.NullValue)
                {
                    errorMessage = $"Inconsistent NullValues attribute for property {propInfo.Name}: {firstValue} and {attr.NullValue}";
                    return false;
                }

                try
                {
                    attr.NullValue.ConvertTo(propInfo.PropertyType);
                } catch (Exception ex)
                {
                    errorMessage = $"Error parsing NullValue attribute for property {propInfo.Name}: {attr.NullValue}; {ex.Message}";
                    return false;
                }
            }
            return true;
        }

        public static bool IsSubclassOfRawGenericType(Type genericType, Type typeToCheck)
        {
            // Used to check for IndexableGrain<,> inheritance; IndexableGrain is a fault-tolerant subclass of IndexableGrainNonFaultTolerant,
            // so we will only see it if the grain's index type is fault tolerant.
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
