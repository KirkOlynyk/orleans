using System;
using Orleans.Runtime;

namespace Orleans.Indexing
{
    internal static class GrainFactoryExtensions
    {
#if false //vv2 unused? overload of GetGrain
        /// <summary>
        /// This method extends GrainFactory by adding a new GetGrain method
        /// that can get the runtime type of the grain interface along
        /// with its primary key, and return the grain casted to an interface
        /// that the given grainInterfaceType extends.
        /// 
        /// The main use-case is when you want to get a grain whose type
        /// is unknown at compile time.
        /// </summary>
        /// <typeparam name="OutputGrainInterfaceType">The output type of
        /// the grain</typeparam>
        /// <param name="gf">the grainFactory instance that this method
        /// is extending its functionality</param>
        /// <param name="grainID">the primary key of the grain</param>
        /// <param name="grainInterfaceType">the runtime type of the grain
        /// interface</param>
        /// <returns>the requested grain with the given grainID and grainInterfaceType</returns>
        public static OutputGrainInterfaceType GetGrain<OutputGrainInterfaceType>(this IGrainFactory gf, string grainID, Type grainInterfaceType) where OutputGrainInterfaceType : IGrain
        {
            Type interfaceType = grainInterfaceType;
            var grainTypeResolver = RuntimeClient.Current.GrainTypeResolver;
            grainTypeResolver.TryGetGrainClassData(interfaceType, out GrainClassData implementation, "");
            var grainId = TypeCodeMapper.ComposeGrainId(implementation, grainID, interfaceType);
            return ((GrainFactory)gf).Cast<OutputGrainInterfaceType>(((GrainFactory)gf)
                .MakeGrainReferenceFromType(interfaceType, grainId)); //vv2 .MakeGrainReferenceFromType was made internal in v1
        }
#endif

        /// <summary>
        /// This method extends GrainFactory by adding a new GetGrain method
        /// to it that can get the runtime type of the grain interface along
        /// with its primary key, and return the grain casted to an interface
        /// that the given grainInterfaceType extends it.
        /// 
        /// The main use-case is when you want to get a grain that its type
        /// is unknown at compile time.
        /// </summary>
        /// <typeparam name="OutputGrainInterfaceType">The output type of
        /// the grain</typeparam>
        /// <param name="gf">the grainFactory instance that this method
        /// is extending its functionality</param>
        /// <param name="grainID">the primary key of the grain</param>
        /// <param name="grainInterfaceType">the runtime type of the grain
        /// interface</param>
        /// <returns>the requested grain with the given grainID and grainInterfaceType</returns>
        public static OutputGrainInterfaceType GetGrain<OutputGrainInterfaceType>(this IGrainFactory gf, IGrainTypeResolver grainTypeResolver, Guid grainID, Type grainInterfaceType)
            where OutputGrainInterfaceType : IGrain
        {
            Type interfaceType = grainInterfaceType;
            GrainClassData implementation;
            grainTypeResolver.TryGetGrainClassData(interfaceType, out implementation, "");
            var grainId = TypeCodeMapper.ComposeGrainId(implementation, grainID, interfaceType);
            return default(OutputGrainInterfaceType); //vv2err: .MakeGrainReferenceFromType was made internal in v1: ((GrainFactory)gf).Cast<OutputGrainInterfaceType>(((GrainFactory)gf).MakeGrainReferenceFromType(interfaceType, grainId));
        }

        /// <summary>
        /// This method extends GrainFactory by adding a new GetGrain method
        /// to it that can get the runtime type of the grain interface along
        /// with its primary key, and return the grain casted to an interface
        /// that the given grainInterfaceType extends it.
        /// 
        /// The main use-case is when you want to get a grain that its type
        /// is unknown at compile time, and also SuperOutputGrainInterfaceType
        /// is non-generic, while outputGrainInterfaceType is a generic type.
        /// </summary>
        /// <typeparam name="SuperOutputGrainInterfaceType">The output type of
        /// the grain</typeparam>
        /// <param name="gf">the grainFactory instance that this method
        /// is extending its functionality</param>
        /// <param name="grainID">the primary key of the grain</param>
        /// <param name="grainInterfaceType">the runtime type of the grain
        /// interface</param>
        /// <param name="outputGrainInterfaceType">the type of grain interface
        /// that should be returned</param>
        /// <returns></returns>
        public static IGrain GetGrain(this IGrainFactory gf, IGrainTypeResolver grainTypeResolver, string grainID, Type grainInterfaceType, Type outputGrainInterfaceType)
        {
            Type interfaceType = grainInterfaceType;
            GrainClassData implementation;
            grainTypeResolver.TryGetGrainClassData(interfaceType, out implementation, "");
            var grainId = TypeCodeMapper.ComposeGrainId(implementation, grainID, interfaceType);
            return null; //vv2err: .MakeGrainReferenceFromType was made internal in v1: (IGrain)((GrainFactory)gf).Cast(((GrainFactory)gf).MakeGrainReferenceFromType(interfaceType, grainId), outputGrainInterfaceType);
        }
    }
}
