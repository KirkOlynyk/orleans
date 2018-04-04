using HelloWorld.Interfaces;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace HelloWorld.Grains
{
    /// <summary>
    /// Orleans grain implementation class HelloGrain.
    /// </summary>
    public class HelloGrain : Orleans.Grain, IHello
    {
        private readonly ILogger logger;

        public HelloGrain(ILogger<HelloGrain> logger)
        {
            this.logger = logger;
        }  

        //Task<string> IHello.SayHello(string greeting)
        public Task<string> SayHello(string greeting)
        {
            logger.LogInformation($"SayHello message received: greeting = '{greeting}'");
            string ans = Orleans.Indexing.Class1.StringId("Hello");
            return Task.FromResult($"You said: '{greeting}', I say: Hello!\nans = '{ans}'");
        }
    }
}
