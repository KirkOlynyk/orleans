using System;
using System.Linq;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;

namespace Orleans.Indexing
{
    /// <summary>
    /// A utility class for the low-level operations related to indexes
    /// </summary>
    public static class IndexUtils
    {
        /// <summary>
        /// A utility function for getting the index grainID,
        /// which is a simple concatenation of the grain
        /// interface type and indexName
        /// </summary>
        /// <param name="grainType">the grain interface type</param>
        /// <param name="indexName">the name of the index, which
        /// is the identifier of the index</param>
        /// <returns>index grainID</returns>
        public static string GetIndexGrainID(Type grainType, string indexName)
        {
            return string.Format("{0}-{1}", TypeUtils.GetFullName(grainType), indexName);
        }

        /// <summary>
        /// This method extracts the name of an index grain from its primary key
        /// </summary>
        /// <param name="index">the given index grain</param>
        /// <returns>the name of the index</returns>
        public static string GetIndexNameFromIndexGrain(IAddressable index)
        {
            string key = index.GetPrimaryKeyString();
            return key.Substring(key.LastIndexOf("-") + 1);
        }

        public static string GetNextIndexBucketIdInChain(IAddressable index)
        {
            string key = index.GetPrimaryKeyString();
            int next = 1;
            int lastDashIndex;
            if (key.Split('-').Length == 3)
            {
                lastDashIndex = key.LastIndexOf("-");
                next = int.Parse(key.Substring(lastDashIndex + 1)) + 1;
                return key.Substring(0, lastDashIndex + 1) + next;
            }
            else
            {
                return key + "-" + next;
            }
        }

#if false //vv2 unused? GetIndexUpdateGenerator
        /// <summary>
        /// This method find the index update generator of a given index
        /// identified by the indexed grain interface type and the name of the index
        /// </summary>
        /// <typeparam name="T">type of the indexed grain interface</typeparam>
        /// <param name="gf">the grain factory instance</param>
        /// <param name="indexName">>the name of the index</param>
        /// <returns>the index update generator of the index</returns>
        public static IIndexUpdateGenerator GetIndexUpdateGenerator<T>(this IGrainFactory gf, string indexName) where T : IIndexableGrain
        {
            if (!(IndexHandler.GetIndexes<T>()).TryGetValue(indexName, out Tuple<object, object, object> output))
                throw new Exception(string.Format("Index \"{0}\" does not exist on {1}.", indexName, typeof(T)));
            return ((IIndexUpdateGenerator)output.Item3);
        }
#endif

        /// <summary>
        /// This method is a central place for finding the
        /// indexes defined on a getter method of a given
        /// grain interface.
        /// </summary>
        /// <typeparam name="IGrainType">the given grain interface type</typeparam>
        /// <param name="grainInterfaceMethod">the getter method on the grain interface</param>
        /// <returns>the name of the index on the getter method of the grain interface</returns>
        public static string GetIndexNameOnInterfaceGetter<IGrainType>(string grainInterfaceMethod)
        {
            return GetIndexNameOnInterfaceGetter(typeof(IGrainType), grainInterfaceMethod);
        }

        /// <summary>
        /// This method is a central place for finding the
        /// indexes defined on a getter method of a given
        /// grain interface.
        /// </summary>
        /// <param name="grainType">the given grain interface type</param>
        /// <param name="grainInterfaceMethod">the getter method on the grain interface</param>
        /// <returns>the name of the index on the getter method of the grain interface</returns>
        public static string GetIndexNameOnInterfaceGetter(Type grainType, string grainInterfaceMethod)
        {
            return "__" + grainInterfaceMethod;
        }

        // The ILoggerFactory implementation creates the category without generic type arguments.
        internal static ILogger CreateLoggerWithFullCategoryName<T>(this ILoggerFactory lf) where T: class
            => lf.CreateLogger(CategoryName(typeof(T)));

        internal static string CategoryName(Type type)
        {
            var genericArgs = type.GetGenericArguments();
            return (genericArgs.Length == 0)
                ? type.Name
                : $"{type.Name.Substring(0, type.Name.IndexOf("`"))}<{string.Join(",", genericArgs.Select(CategoryName))}>";
        }
    }
}
