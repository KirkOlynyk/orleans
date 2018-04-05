using System;
using Orleans.Runtime;

namespace Orleans.Indexing
{
    public static class GrainExtensions
    {
        /// <summary>
        /// Converts this grain to a specific grain interface.
        /// </summary>
        /// <typeparam name="TGrainInterface">The type of the grain interface.</typeparam>
        /// <param name="grain">The grain to convert.</param>
        /// <param name="gf">the grain factory object</param>
        /// <returns>A strongly typed <c>GrainReference</c> of grain interface type TGrainInterface.</returns>
        public static TGrainInterface AsReference<TGrainInterface>(this IAddressable grain, IGrainFactory gf)
        {
            return (grain != null)
                ? ((GrainFactory)gf).Cast<TGrainInterface>(grain.AsWeaklyTypedReference())
                : throw new ArgumentNullException("grain", "Cannot pass null as an argument to AsReference");
        }

        /// <summary>
        /// Converts this grain to the grain interface identified by iGrainType.
        /// 
        /// Finally, it casts it to the type provided as TGrainInterface.
        /// The caller should make sure that iGrainType extends TGrainInterface.
        /// </summary>
        /// <typeparam name="TGrainInterface">output grain interface type, which iGrainType extends it</typeparam>
        /// <param name="grain">the target grain to be casted</param>
        /// <param name="gf">the grain factory object</param>
        /// <param name="iGrainType">the grain implementation type</param>
        /// <returns>A strongly typed <c>GrainReference</c> of grain interface type iGrainType casted to TGrainInterface.</returns>
        /// <returns></returns>
        public static TGrainInterface AsReference<TGrainInterface>(this IAddressable grain, IGrainFactory gf, Type iGrainType)
        {
            return (grain != null)
                ? (TGrainInterface)((GrainFactory)gf).Cast(grain.AsWeaklyTypedReference(), iGrainType)
                : throw new ArgumentNullException("grain", "Cannot pass null as an argument to AsReference");
        }

        private const string WRONG_GRAIN_ERROR_MSG = "Passing a half baked grain as an argument. It is possible that you instantiated a grain class explicitly, as a regular object and not via Orleans runtime or via proper test mocking";

        internal static GrainReference AsWeaklyTypedReference(this IAddressable grain)
        {
            // When called against an instance of a grain reference class, do nothing
            if (grain is GrainReference reference) return reference;

            if (grain is Grain grainBase)
            {
                return grainBase.Data?.GrainReference != null
                    ? grainBase.Data.GrainReference
                    : throw new ArgumentException(WRONG_GRAIN_ERROR_MSG, "grain");
            }

            if (grain is ISystemTargetBase systemTarget)
                return GrainReference.FromGrainId(systemTarget.GrainId, systemTarget.GrainReferenceRuntime, null, systemTarget.Silo);

            throw new ArgumentException(string.Format("AsWeaklyTypedReference has been called on an unexpected type: {0}.", grain.GetType().FullName), "grain");
        }
    }
}
