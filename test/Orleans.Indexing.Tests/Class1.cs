using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using Orleans;
using Orleans.Runtime;
using Microsoft.Extensions.Logging;

namespace Orleans.Indexing.Tests
{
    public interface ISimpleGrain : IGrainWithIntegerKey
    {
        Task<string> GetName();
    }

    public class Class1
    {
        private readonly ITestOutputHelper output;

        public Class1(ITestOutputHelper output)
        {
            this.output = output;
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


        [Fact]
        public void PassingTest()
        {
            output.WriteLine("First Indexing Test!");
            string assembly_file_path = @"D:\Orleans\orleans\src\Orleans.Indexing\bin\Debug\netstandard2.0\Orleans.Indexing.dll";
            if (System.IO.File.Exists(assembly_file_path))
            {
                System.Reflection.Assembly asm = System.Reflection.Assembly.LoadFrom(assembly_file_path);
                System.Type[] types = asm.GetTypes();
                foreach (Type t in types)
                {
                    output.WriteLine(t.Name);
                }
            }
            Assert.Equal(1, 1);
        }
    }
}
