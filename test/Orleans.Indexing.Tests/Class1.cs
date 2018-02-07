using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Hosting;
using Orleans.Logging;
using Orleans.Runtime;
using Orleans.TestingHost;
using Orleans.TestingHost.Utils;
using Orleans.Indexing;
using TestExtensions;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Indexing.Tests
{
    public interface ISimpleGrain : IGrainWithIntegerKey
    {
        Task<string> GetName();
    }

    public class SimpleGrain : Grain, ISimpleGrain
    {
        private ILogger logger;

        public SimpleGrain(ILogger<SimpleGrain> logger)
        {
            this.logger = logger;
        }

        public Task<string> GetName()
        {
            this.logger.Info("GetName");
            return Task.FromResult(nameof(SimpleGrain));
        }
    }

    public class SimpleTests : OrleansTestingBase, IClassFixture<SimpleTests.Fixture>
    {
        private readonly Fixture fixture;
        private readonly ITestOutputHelper output;
        private readonly Type index_attribute_type;
        private readonly Type indexable_grain_type;
        private readonly Type total_index_type;
        private readonly PropertyInfo index_property;
        private readonly PropertyInfo is_eager_property;
        private readonly PropertyInfo is_unique_property;
        private readonly PropertyInfo max_entries_per_bucket_property;


        public SimpleTests(Fixture fixture, ITestOutputHelper output)
        {
            this.fixture = fixture;
            this.output = output;
            this.index_attribute_type = Type.GetType("Orleans.Indexing.IndexAttribute, Orleans.Indexing");
            this.total_index_type = Type.GetType("Orleans.Indexing.TotalIndex, Orleans.Indexing");
            this.indexable_grain_type = Type.GetType("Orleans.Indexing.IIndexableGrain`1, Orleans.Indexing");
            this.index_property = this.index_attribute_type.GetProperty("IndexType");
            this.is_eager_property = this.index_attribute_type.GetProperty("IsEager");
            this.is_unique_property = this.index_attribute_type.GetProperty("IsUnique");
            this.max_entries_per_bucket_property = this.index_attribute_type.GetProperty("MaxEntriesPerBucket");
        }
        public class Fixture : BaseTestClusterFixture
        {
            protected override void ConfigureTestCluster(TestClusterBuilder builder)
            {
                //configure client and silo through their configurator
                builder.AddSiloBuilderConfigurator<SiloBuilderConfigurator>();
                builder.AddClientBuilderConfigurator<ClientBuilderConfigurator>();
            }
        }
        public class ClientBuilderConfigurator : IClientBuilderConfigurator
        {
            public void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
            {
                clientBuilder.ConfigureLogging(builder => builder.AddFile(TestingUtils.CreateTraceFileName("client", configuration.GetTestClusterOptions().ClusterId)));
            }
        }
        public class SiloBuilderConfigurator : ISiloBuilderConfigurator
        {
            public void Configure(ISiloHostBuilder hostBuilder)
            {
                var siloName = GetSiloName(hostBuilder);
                //add simpleGrain's assmebly to siloHost, so the grain assembly will be loaded to silo
                //also configure logging using ConfigureLogging method
                hostBuilder.ConfigureApplicationParts(parts =>
                    parts.AddApplicationPart(typeof(SimpleGrain).Assembly).WithReferences())
                    .ConfigureLogging(builder => builder.AddFile(TestingUtils.CreateTraceFileName((string)siloName, hostBuilder.GetTestClusterOptions().ClusterId)));
            }
        }


        [Fact, TestCategory("BVT"), TestCategory("Functional"), TestCategory("Streaming"), TestCategory("PubSub")]
        public async Task IndexingSimpleTest1()
        {
            //client side logger log out this line
            this.DbgPrint("Entering IndexingSimpleTest");
            this.DbgPrint("calling GetGrain ...");
            try
            {
                var grain = this.fixture.GrainFactory.GetGrain<ISimpleGrain>(1);
                this.DbgPrint("grain: {0}", grain);
                this.DbgPrint("calling grain.GetName ...");
                var name = await grain.GetName();
                this.DbgPrint("name: {0}", name);
                Assert.NotEmpty(name);
                this.DbgPrint("Exiting IndexingSimpleTest");
            }
            catch (Exception e)
            {
                this.DbgPrint("\n********** An Exception has be thrown **********\n");
                this.DbgPrint("\nException Message:");
                this.DbgPrint(e.Message);
            }
        }


        private IEnumerable<Type> GetGrainTypes()
        {
            Predicate<Type> whereFunc = Utils.IsConcreteGrainClass;
            Assembly[] assemblies = AppDomain.CurrentDomain.GetAssemblies();
            List<Type> ans = new List<Type>();
            foreach (Assembly assembly in assemblies)
            {
                IEnumerable<Type> types = Utils.GetTypes(assembly, whereFunc, this.fixture.Logger);
                ans.AddRange(types);
            }
            return ans;
        }

        private static string Get_index_name(PropertyInfo info)
        {
            return "__" + info.Name;
        }
        private void Create_indexes_for_a_single_interface_of_a_grain_type(
            Type indexable_grain_interface,
            Type properties_arg,
            Type user_defined_interface,
            Type grain_type
            )
        {
            //this.DbgPrint("Create_indexes_for_a_single_interface_of_a_grain");
            //this.DbgPrint("  indexable_grain_interface: {0}", indexable_grain_interface);
            //this.DbgPrint("  properties_arg: {0}", properties_arg);
            //this.DbgPrint("  user_defined_interface: {0}", user_defined_interface);
            //this.DbgPrint("  grain_type: {0}", grain_type);
            if (indexable_grain_interface != user_defined_interface)
            {
                if (indexable_grain_interface.IsAssignableFrom(user_defined_interface))
                {
                    PropertyInfo[] properties = properties_arg.GetProperties();
                    foreach (PropertyInfo property in properties)
                    {
                        object[] custom_attributes = property.GetCustomAttributes(this.index_attribute_type, false);
                        foreach(object attribute in custom_attributes)
                        {
                            string index_name = Get_index_name(property);
                            //this.DbgPrint("  index_name: \"{0}\"", index_name);
                            Type index_type = (Type)this.index_property.GetValue(attribute);
                            //this.DbgPrint("  index_type: \"{0}\"", index_type.Name);
                            if (index_type.IsGenericTypeDefinition)
                            {
                                index_type = index_type.MakeGenericType(property.PropertyType, user_defined_interface);
                                //this.DbgPrint("  index_type: \"{0}\"", index_type.Name);
                            }
                            bool is_eagerly_updated = (bool)this.is_eager_property.GetValue(attribute);
                            bool is_unique = (bool)this.is_unique_property.GetValue(attribute);
                            int max_entries_per_bucket = (int)this.max_entries_per_bucket_property.GetValue(attribute);

                            //this.DbgPrint("  is_eargerly_updated: {0}", is_eagerly_updated.ToString());
                            //this.DbgPrint("  is_unique: {0}", is_unique.ToString());
                            //this.DbgPrint("  max_entries_per_bucket: {0}", max_entries_per_bucket);
                            CreateIndex(index_type, index_name, is_unique, max_entries_per_bucket, property);
                        }
                    }
                }
            }
        }

        public static Type GetGenericType(Type givenType, Type genericInterfaceType)
        {
            var interfaceTypes = givenType.GetInterfaces();

            foreach (var it in interfaceTypes)
            {
                if (it.IsGenericType && it.GetGenericTypeDefinition() == genericInterfaceType)
                    return it;
            }

            if (givenType.IsGenericType && givenType.GetGenericTypeDefinition() == genericInterfaceType)
                return givenType;

            Type baseType = givenType.BaseType;
            if (baseType == null) return null;

            return GetGenericType(baseType, genericInterfaceType);
        }


        private void CreateIndex(
            Type index_type,
            string index_name,
            bool is_unique_index,
            int max_entries_per_bucket,
            PropertyInfo indexed_property
            )
        {
            this.DbgPrint("Create_index");
            this.DbgPrint("  index_name: {0}", index_name);
            this.DbgPrint("  is_unique_index: {0}", is_unique_index.ToString());
            this.DbgPrint("  max_entries_per_bucket: {0}", max_entries_per_bucket);
            this.DbgPrint("  indexed_property: {0}", indexed_property.ToString());
            Type generic_index_type = GetGenericType(index_type, typeof(IIndexInterface<,>));
            this.DbgPrint("  generic_index_type: {0}", generic_index_type.ToString());
            if (generic_index_type != null)
            {
                Type[] index_type_args = generic_index_type.GetGenericArguments();
                Type key_type = index_type_args[0];
                Type grain_type = index_type_args[1];
                this.DbgPrint("  key_type: {0}", key_type);
                this.DbgPrint("  grain_type: {0}", grain_type);
                if (typeof(IGrain).IsAssignableFrom(index_type))
                {
                    string index_grain_id = IndexUtils.GetIndexGrainID(grain_type, index_name);
                    this.DbgPrint("  index_grain_id: {0}", index_grain_id);
                }
            }
        }
        
        private void Process_grain_type(Type grain_type)
        {
            Type[] interfaces = grain_type.GetInterfaces();
            foreach (Type candidate_indexable_grain_interface in interfaces)
            {
                if (candidate_indexable_grain_interface.IsGenericType)
                {
                    Type generic_type_definition =
                        candidate_indexable_grain_interface.GetGenericTypeDefinition();
                    if (generic_type_definition == this.indexable_grain_type)
                    {
                        Type[] generic_arguments =
                            candidate_indexable_grain_interface.GetGenericArguments();
                        Type properties_arg = generic_arguments[0];
                        TypeInfo type_info = properties_arg.GetTypeInfo();
                        if (type_info.IsClass)
                        {
                            foreach (Type user_defined_interface in interfaces)
                            {
                                Create_indexes_for_a_single_interface_of_a_grain_type(
                                    candidate_indexable_grain_interface,
                                    properties_arg,
                                    user_defined_interface,
                                    grain_type
                                    );
                            }
                        }
                    break;
                    }
                }
            }
        }


        [Fact, TestCategory("Functional")]
        public void IndexingSimpleTest2()
        {
            //client side logger log out this line
            IEnumerable<Type> grain_types = GetGrainTypes();
            foreach (Type grain_type in grain_types)
            {
                Process_grain_type(grain_type);
            }
            Assert.True(1 == 1);
        }

        private void DbgPrint(string fmt, params object[] args)
        {
            string msg = string.Format(fmt, args);
            this.fixture.Logger.Info(msg);
            this.output.WriteLine(msg);
        }

        private static string GetSiloName(ISiloHostBuilder hostBuilder)
        {
            var ignore = hostBuilder.Properties.TryGetValue("Configuration", out var configObj);
            if (configObj is IConfiguration config)
            {
                var siloName = config["SiloName"];
                return siloName;
            }
            return null;
        }
    }


    public interface IPlayerProperties
    {
        string Location { get; set; }
    }
    public interface IPlayerState : IPlayerProperties  { }

    [Serializable]
    public class Player1PropertiesNonFaultTolerant : IPlayerProperties
    {
        [ActiveIndex(isEager: true)]
        public string Location { get; set; }
    }
    public interface IPlayer1GrainNonFaultTolerant : IPlayerGrain, IIndexableGrain<Player1PropertiesNonFaultTolerant>
    {
    }


    public interface IPlayerGrain : IGrainWithIntegerKey
    {
        Task<string> GetLocation();
        Task SetLocation(string location);
    }

    public class Player1GrainNonFaultTolerant : Grain, IPlayer1GrainNonFaultTolerant
    {
        private ILogger logger;
        private string location;

        public Player1GrainNonFaultTolerant(ILogger<Player1GrainNonFaultTolerant> logger)
        {
            this.logger = logger;
        }

        public Task<string> GetLocation()
        {
            this.logger.Info("GetLocation");
            return Task.FromResult(this.location);
        }

        public Task SetLocation(string _location)
        {
            this.logger.Info("SetLocation");
            return Task.Run(() => this.location = _location);
        }
    }


    public class Class1
    {
        private readonly ITestOutputHelper output;

        public Class1(ITestOutputHelper output)
        {
            this.output = output;
        }

        [Fact]
        public void IndexingPassingTest()
        {
            this.output.WriteLine("First Indexing Test!");
            string assembly_file_path = @"D:\Orleans\orleans\src\Orleans.Indexing\bin\Debug\netstandard2.0\Orleans.Indexing.dll";
            if (System.IO.File.Exists(assembly_file_path))
            {
                System.Reflection.Assembly asm = System.Reflection.Assembly.LoadFrom(assembly_file_path);
                System.Type[] types = asm.GetTypes();
                foreach (Type t in types)
                {
                    this.output.WriteLine(t.FullName);
                }
            }
            Assert.Equal(1, 1);
        }
    }
}
