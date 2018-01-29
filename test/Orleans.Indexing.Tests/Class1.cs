using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Orleans.Indexing.Tests
{
    public class Class1
    {
        [Fact]
        public static void PassingTest()
        {
            Console.WriteLine("First Indexing Test!");
            Assert.Equal(1, 1);
        }
    }
}
