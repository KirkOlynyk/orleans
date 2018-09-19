using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.TestingHost;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using TestExtensions;
using UnitTests.GrainInterfaces;
using UnitTests.Grains;

namespace Orleans.Indexing.Tests
{
    public abstract class BaseIndexingFixture : BaseTestClusterFixture
    {
        protected TestClusterBuilder ConfigureTestClusterForIndexing(TestClusterBuilder builder)
        {
            // Currently nothing
            return builder;
        }

        internal static ISiloHostBuilder Configure(ISiloHostBuilder hostBuilder, string databaseName = null)
        {
            if (!TestDefaultConfiguration.GetValue("CosmosDBEndpoint", out string cosmosDBEndpoint)
                || !TestDefaultConfiguration.GetValue("CosmosDBKey", out string cosmosDBKey))
            {
                throw new IndexConfigurationException("CosmosDB connection values are not specified");
            }

            hostBuilder.AddMemoryGrainStorage(IndexingTestConstants.GrainStore)
                       .AddMemoryGrainStorage(IndexingTestConstants.MemoryStore)
                       .AddMemoryGrainStorage("PubSubStore") // PubSubStore service is run for silo startup
                       .AddMemoryGrainStorage(IndexingConstants.INDEXING_WORKFLOWQUEUE_STORAGE_PROVIDER_NAME)
                       .AddMemoryGrainStorage(IndexingConstants.INDEXING_STORAGE_PROVIDER_NAME)
                       .AddSimpleMessageStreamProvider(IndexingConstants.INDEXING_STREAM_PROVIDER_NAME)
                       .ConfigureLogging(loggingBuilder =>
                       {
                           loggingBuilder.SetMinimumLevel(LogLevel.Information);
                           loggingBuilder.AddDebug();
                       })
                       .ConfigureApplicationParts(parts =>
                       {
                           parts.AddApplicationPart(typeof(BaseIndexingFixture).Assembly);
                           parts.AddApplicationPart(typeof(ISimpleGrain).Assembly);
                           parts.AddApplicationPart(typeof(SimpleGrain).Assembly);
                       });

            return databaseName != null
                ? hostBuilder.AddCosmosDBGrainStorage(IndexingTestConstants.CosmosDBGrainStorage, opt =>
                    {
                        opt.AccountEndpoint = cosmosDBEndpoint;
                        opt.AccountKey = cosmosDBKey;
                        opt.ConnectionMode = Microsoft.Azure.Documents.Client.ConnectionMode.Gateway;
                        opt.DropDatabaseOnInit = true;
                        opt.AutoUpdateStoredProcedures = true;
                        opt.CanCreateResources = true;
                        opt.DB = databaseName;
                        opt.InitStage = ServiceLifecycleStage.RuntimeStorageServices;
                        opt.StateFieldsToIndex.AddRange(GetStateFieldsToIndex());
                    })
                : hostBuilder;
        }

        internal static IClientBuilder Configure(IClientBuilder clientBuilder)
        {
            return clientBuilder.AddSimpleMessageStreamProvider(IndexingConstants.INDEXING_STREAM_PROVIDER_NAME)
                                .ConfigureLogging(loggingBuilder =>
                                {
                                    loggingBuilder.SetMinimumLevel(LogLevel.Information);
                                    loggingBuilder.AddDebug();
                                })
                                .ConfigureApplicationParts(parts =>
                                {
                                    parts.AddApplicationPart(typeof(BaseIndexingFixture).Assembly);
                                    parts.AddApplicationPart(typeof(ISimpleGrain).Assembly);
                                    parts.AddApplicationPart(typeof(SimpleGrain).Assembly);
                                });
        }

        private static IEnumerable<string> GetStateFieldsToIndex()
        {
            var grainTypes = typeof(BaseIndexingFixture).Assembly.GetConcreteGrainClasses(logger: null).ToArray();
            var interfaces = new Dictionary<Type, string[]>();
            foreach (var grainType in grainTypes)
            {
                bool isFaultTolerant = IsSubclassOfRawGenericType(typeof(IndexableGrain<,>), grainType);
                GetFieldsForASingleGrainType(grainType, interfaces, isFaultTolerant ? "UserState." : string.Empty);
            }
            return new HashSet<string>(interfaces.SelectMany(kvp => kvp.Value));
        }

        private static void GetFieldsForASingleGrainType(Type grainType, Dictionary<Type, string[]> interfaces, string fieldPrefix)
        {
            Type[] grainInterfaces = grainType.GetInterfaces();

            // If there is an interface that directly extends IIndexableGrain<TProperties>...
            Type iIndexableGrain = grainInterfaces.Where(itf => itf.IsGenericType && itf.GetGenericTypeDefinition() == typeof(IIndexableGrain<>)).FirstOrDefault();
            if (iIndexableGrain != null)
            {
                // ... and its generic argument is a class (TProperties)... 
                Type propertiesArgType = iIndexableGrain.GetGenericArguments()[0];
                if (propertiesArgType.GetTypeInfo().IsClass)
                {
                    // ... then examine all indexed fields for all the descendant interfaces of IIndexableGrain<TProperties> (these interfaces are defined by end-users).
                    foreach (Type userDefinedIGrain in grainInterfaces.Where(itf => iIndexableGrain != itf && iIndexableGrain.IsAssignableFrom(itf)
                                                                                 && !interfaces.ContainsKey(itf)))
                    {
                        GetFieldsForASingleInterface(interfaces, propertiesArgType, userDefinedIGrain, grainType, fieldPrefix);
                    }
                }
            }
        }

        private static void GetFieldsForASingleInterface(Dictionary<Type, string[]> interfaces, Type propertiesArgType,
                                                         Type userDefinedIGrain, Type userDefinedGrainImpl, string fieldPrefix)
        {
            // All the properties in TProperties are scanned for StorageManagedIndex annotation.
            var fields = new List<string>();
            foreach (PropertyInfo propInfo in propertiesArgType.GetProperties())
            {
                var indexAttr = propInfo.GetCustomAttributes<StorageManagedIndexAttribute>(inherit: false).FirstOrDefault();
                if (indexAttr != null)
                {
                    fields.Add(fieldPrefix + propInfo.Name);
                }
            }

            if (fields.Count > 0)
            {
                interfaces[userDefinedIGrain] = fields.ToArray();
            }
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
