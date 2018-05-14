using System;
using Orleans.Runtime;

namespace Orleans.Indexing
{
    internal static class GrainFactoryExtensions
    {
        /// <summary>
        /// This method extends GrainFactory by adding a new GetGrain method to it that can get the runtime type of the grain interface along
        /// with its primary key, and return the grain casted to an interface that the given grainInterfaceType extends it.
        /// 
        /// The main use-case is when you want to get a grain whose type is unknown at compile time.
        /// </summary>
        /// <typeparam name="OutputGrainInterfaceType">The output type of the grain</typeparam>
        /// <param name="siloIndexManager">the Index manager for this silo</param>
        /// <param name="grainPrimaryKey">the primary key of the grain</param>
        /// <param name="grainInterfaceType">the runtime type of the grain interface</param>
        /// <returns>the requested grain with the given grainID and grainInterfaceType</returns>
        public static OutputGrainInterfaceType GetGrain<OutputGrainInterfaceType>(this SiloIndexManager siloIndexManager, Guid grainPrimaryKey, Type grainInterfaceType)
            where OutputGrainInterfaceType : IGrain
            => siloIndexManager.GetGrain<OutputGrainInterfaceType>(grainPrimaryKey, grainInterfaceType);

        /// <summary>
        /// This method extends GrainFactory by adding a new GetGrain method to it that can get the runtime type of the grain interface along
        /// with its primary key, and return the grain casted to an interface that the given grainInterfaceType extends it.
        /// 
        /// The main use-case is when you want to get a grain whose type is unknown at compile time, and also SuperOutputGrainInterfaceType
        /// is non-generic, while outputGrainInterfaceType is a generic type.
        /// </summary>
        /// <param name="siloIndexManager">the Index manager for this silo</param>
        /// <param name="grainPrimaryKey">the primary key of the grain</param>
        /// <param name="grainInterfaceType">the runtime type of the grain interface</param>
        /// <param name="outputGrainInterfaceType">the type of grain interface that should be returned</param>
        /// <returns></returns>
        public static IGrain GetGrain(this SiloIndexManager siloIndexManager, string grainPrimaryKey, Type grainInterfaceType, Type outputGrainInterfaceType)
            => siloIndexManager.GetGrain(grainPrimaryKey, grainInterfaceType, outputGrainInterfaceType);
    }
}
