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
        /// A utility function for getting the index grainID, which is a simple concatenation of the grain
        /// interface type and indexName
        /// </summary>
        /// <param name="grainType">the grain interface type</param>
        /// <param name="indexName">the name of the index, which is the identifier of the index</param>
        /// <returns>index grainID</returns>
        public static string GetIndexGrainPrimaryKey(Type grainType, string indexName)
            => string.Format("{0}-{1}", GetFullTypeName(grainType), indexName);

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
            if (key.Split('-').Length == 3)
            {
                int lastDashIndex = key.LastIndexOf("-");
                next = int.Parse(key.Substring(lastDashIndex + 1)) + 1;
                return key.Substring(0, lastDashIndex + 1) + next;
            }
            return key + "-" + next;
        }

        /// <summary>
        /// This method is a central place for finding the indexes defined on a getter method of a given
        /// grain interface.
        /// </summary>
        /// <typeparam name="IGrainType">the given grain interface type</typeparam>
        /// <param name="grainInterfaceMethod">the getter method on the grain interface</param>
        /// <returns>the name of the index on the getter method of the grain interface</returns>
        public static string GetIndexNameOnInterfaceGetter<IGrainType>(string grainInterfaceMethod)
            => GetIndexNameOnInterfaceGetter(typeof(IGrainType), grainInterfaceMethod);

        /// <summary>
        /// This method is a central place for finding the indexes defined on a getter method of a given
        /// grain interface.
        /// </summary>
        /// <param name="grainType">the given grain interface type</param>
        /// <param name="grainInterfaceMethod">the getter method on the grain interface</param>
        /// <returns>the name of the index on the getter method of the grain interface</returns>
        public static string GetIndexNameOnInterfaceGetter(Type grainType, string grainInterfaceMethod)
            => "__" + grainInterfaceMethod;

        // The ILoggerFactory implementation creates the category without generic type arguments.
        internal static ILogger CreateLoggerWithFullCategoryName<T>(this ILoggerFactory lf) where T: class
            => lf.CreateLogger(GetFullTypeName(typeof(T), expandArgNames:true));

        internal static string GetFullTypeName(Type type, bool expandArgNames = false)
        {
            var name = type.FullName ?? (type.IsGenericParameter ? type.Name : type.Namespace + "." + type.Name);
            var assemblyInfoStart = name.IndexOf("[[");
            if (assemblyInfoStart > 0) name = name.Substring(0, assemblyInfoStart);
            var genericArgs = type.GetGenericArguments();
            return (genericArgs.Length == 0 || !expandArgNames)
                ? name
                : $"{name.Substring(0, name.IndexOf("`"))}<{string.Join(",", genericArgs.Select(arg => GetFullTypeName(arg, true)))}>";
        }
    }
}
