using Orleans.Runtime;
using System;

namespace Orleans.Indexing
{
    /// <summary>
    /// Type Code Mapping functions.
    /// </summary>
    internal static class TypeCodeMapper
    {
#if false //vv2 GetImplementation overloads not used?
        internal static GrainClassData GetImplementation(IGrainTypeResolver grainTypeResolver, Type interfaceType, string grainClassNamePrefix = null)
        {
            if (!grainTypeResolver.TryGetGrainClassData(interfaceType, out GrainClassData implementation, grainClassNamePrefix))
            {
                var loadedAssemblies = grainTypeResolver.GetLoadedGrainAssemblies();
                throw new ArgumentException(
                    string.Format("Cannot find an implementation class for grain interface: {0}{2}. Make sure the grain assembly was correctly deployed and loaded in the silo.{1}",
                                  interfaceType,
                                  string.IsNullOrEmpty(loadedAssemblies) ? string.Empty : string.Format(" Loaded grain assemblies: {0}", loadedAssemblies),
                                  string.IsNullOrEmpty(grainClassNamePrefix) ? string.Empty : ", grainClassNamePrefix=" + grainClassNamePrefix));
            }
            return implementation;
        }

        internal static GrainClassData GetImplementation(int interfaceId, string grainClassNamePrefix = null)
        {
            var grainTypeResolver = RuntimeClient.Current.GrainTypeResolver;    //vv2if RuntimeClient.Current
            if (grainTypeResolver.TryGetGrainClassData(interfaceId, out GrainClassData implementation, grainClassNamePrefix)) return implementation;

            var loadedAssemblies = grainTypeResolver.GetLoadedGrainAssemblies();
            throw new ArgumentException(
                string.Format("Cannot find an implementation class for grain interface: {0}{2}. Make sure the grain assembly was correctly deployed and loaded in the silo.{1}",
                    interfaceId,
                    string.IsNullOrEmpty(loadedAssemblies) ? string.Empty : string.Format(" Loaded grain assemblies: {0}", loadedAssemblies),
                    string.IsNullOrEmpty(grainClassNamePrefix) ? string.Empty : ", grainClassNamePrefix=" + grainClassNamePrefix));

        }

        internal static GrainClassData GetImplementation(string grainImplementationClassName)
        {
            var grainTypeResolver = RuntimeClient.Current.GrainTypeResolver;
            if (!grainTypeResolver.TryGetGrainClassData(grainImplementationClassName, out GrainClassData implementation))
                throw new ArgumentException(string.Format("Cannot find an implementation grain class: {0}. Make sure the grain assembly was correctly deployed and loaded in the silo.", grainImplementationClassName));

            return implementation;
        }
#endif

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

        internal static GrainId ComposeGrainId(GrainClassData implementation, long primaryKey, Type interfaceType, string keyExt = null)
        {
            return GrainId.GetGrainId(implementation.GetTypeCode(interfaceType), primaryKey, keyExt);
        }

        internal static GrainId ComposeGrainId(GrainClassData implementation, string primaryKey, Type interfaceType)
        {
            return GrainId.GetGrainId(implementation.GetTypeCode(interfaceType), primaryKey);
        }

    }
}
