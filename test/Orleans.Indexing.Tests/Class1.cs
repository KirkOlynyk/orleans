using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Hosting;
using Orleans.Logging;
using Orleans.Runtime;
using Orleans.TestingHost;
using Orleans.TestingHost.Utils;
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

        public SimpleTests(Fixture fixture)
        {
            this.fixture = fixture;
        }

        [Fact, TestCategory("BVT"), TestCategory("Functional"), TestCategory("Streaming"), TestCategory("PubSub")]
        public async Task SimpleTest()
        {
            //client side logger log out this line
            this.fixture.Logger.Info("Start test SimpleTest");
            var grain = this.fixture.GrainFactory.GetGrain<ISimpleGrain>(1);
            var name = await grain.GetName();
            Assert.NotEmpty(name);
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



    public class Class1
    {
        private readonly ITestOutputHelper output;




        public Class1(ITestOutputHelper output)
        {
            this.output = output;
        }

        [Fact]
        public void PassingTest()
        {
            this.output.WriteLine("First Indexing Test!");
            string assembly_file_path = @"D:\Orleans\orleans\src\Orleans.Indexing\bin\Debug\netstandard2.0\Orleans.Indexing.dll";
            if (System.IO.File.Exists(assembly_file_path))
            {
                System.Reflection.Assembly asm = System.Reflection.Assembly.LoadFrom(assembly_file_path);
                System.Type[] types = asm.GetTypes();
                foreach (Type t in types)
                {
                    this.output.WriteLine(t.Name);
                }
            }
            Assert.Equal(1, 1);
        }
    }
}
