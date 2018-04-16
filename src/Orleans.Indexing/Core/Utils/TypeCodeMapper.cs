using Orleans.Runtime;
using System;

namespace Orleans.Indexing
{
    /// <summary>
    /// Type Code Mapping functions.
    /// </summary>
    internal static class TypeCodeMapper
    {
        internal static GrainClassData GetImplementation(IRuntimeClient runtimeClient, Type grainImplementationClass)
        {
            var grainTypeResolver = runtimeClient.GrainTypeResolver;
            if (!grainTypeResolver.TryGetGrainClassData(grainImplementationClass, out GrainClassData implementation, ""))
                throw new ArgumentException(string.Format("Cannot find an implementation grain class: {0}. Make sure the grain assembly was correctly deployed and loaded in the silo.", grainImplementationClass));

            return implementation;
        }

        internal static GrainId ComposeGrainId(GrainClassData implementation, Guid primaryKey, Type interfaceType, string keyExt = null)
        {
            return GrainId.GetGrainId(implementation.GetTypeCode(interfaceType), primaryKey, keyExt);
        }

        internal static GrainId ComposeGrainId(GrainClassData implementation, string primaryKey, Type interfaceType)
        {
            return GrainId.GetGrainId(implementation.GetTypeCode(interfaceType), primaryKey);
        }

    }
}
