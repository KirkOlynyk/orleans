using System;
using Orleans.Runtime;

namespace Orleans.Indexing
{
    /// <summary>
    /// Type Code Mapping functions.
    /// </summary>
    internal static class TypeCodeMapper
    {
        internal static GrainClassData GetImplementation(IGrainTypeResolver grainTypeResolver, Type grainImplementationClass)
            => (grainTypeResolver.TryGetGrainClassData(grainImplementationClass, out GrainClassData implementation, ""))
                ? implementation
                : throw new ArgumentException($"Cannot find an implementation grain class: {grainImplementationClass}." +
                                              $" Make sure the grain assembly was correctly deployed and loaded in the silo.");
    }
}
