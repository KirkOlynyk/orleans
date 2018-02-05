using System;
using System.Reflection;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;


namespace Orleans.Indexing
{
    public class Class1
    {
        /// <summary>
        /// Stub function for testing. The string identity function.
        /// </summary>
        /// <param name="input">Input string</param>
        /// <returns>Output string the same object as the input string</returns>
        public static string StringId(string input)
        {
            return input;
        }
    }

    public class Utils
    {
        public static TGrainInterface GetGrain<TGrainInterface>(string primaryKey, string grainClassNamePrefix = null) where TGrainInterface : IGrainWithStringKey
        {
            throw new NotImplementedException();
        }

        public static IEnumerable<Type> GetTypes(Assembly assembly, Predicate<Type> whereFunc, ILogger logger)
        {
            return Orleans.Runtime.TypeUtils.GetTypes(assembly, whereFunc, logger);
        }

        public static bool IsConcreteGrainClass(Type type)
        {
            return Orleans.Runtime.TypeUtils.IsConcreteGrainClass(type);
        }


    }
}
