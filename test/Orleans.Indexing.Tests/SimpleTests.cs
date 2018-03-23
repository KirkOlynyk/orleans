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
            this.logger.Info("SimpleGrain constructor has been called.");
        }

        public Task<string> GetName()
        {
            this.logger.Info("GetName has been called!");
            return Task.FromResult(nameof(SimpleGrain));
        }
    }

    public class SimpleTests : OrleansTestingBase, IClassFixture<SimpleTests.Fixture>
    {
        private readonly Fixture fixture;
        private readonly ITestOutputHelper output;

        public SimpleTests(Fixture fixture, ITestOutputHelper output)
        {
            this.fixture = fixture;
            this.output = output;
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
                clientBuilder.ConfigureLogging(builder =>
                        builder.AddFile(TestingUtils.CreateTraceFileName("client",configuration.GetTestClusterOptions().ClusterId)));
            }
        }
        public class SiloBuilderConfigurator : ISiloBuilderConfigurator
        {
            public void Configure(ISiloHostBuilder hostBuilder)
            {
                var siloName = GetSiloName(hostBuilder);

                //add simpleGrain's assembly to siloHost, so the grain assembly will be loaded to silo
                //also configure logging using ConfigureLogging method
                hostBuilder.ConfigureApplicationParts(parts =>
                    parts.AddApplicationPart(typeof(SimpleGrain).Assembly).WithReferences())
                    .ConfigureLogging(builder =>
                        builder.AddFile(TestingUtils.CreateTraceFileName(siloName,hostBuilder.GetTestClusterOptions().ClusterId)));
            }
        }


        [Fact, TestCategory("Indexing")]
        public async Task Test1()
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
}
