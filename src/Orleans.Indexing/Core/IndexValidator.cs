using Orleans.Runtime;
using System;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace Orleans.Indexing
{
    public static class IndexValidator
    {
        public static async Task Validate(Type[] grainTypes)
        {
            var _ = await ApplicationPartsIndexableGrainLoader.GetIndexRegistry(null, grainTypes);
        }

        public static async Task Validate(Assembly assembly)
        {
            await Validate(assembly.GetConcreteGrainClasses(logger: null).ToArray());
        }
    }
}
