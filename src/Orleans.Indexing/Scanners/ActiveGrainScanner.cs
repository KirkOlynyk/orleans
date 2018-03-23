using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.Indexing
{
    public class ActiveGrainScanner
    {
#if false // vv2  -- are these needed or are they already all done by ActiveGrainEnumeratorGrain?
        public static async Task<IEnumerable<T>> GetActiveGrains<T>() where T : IGrain
        {
            string grainTypeName = TypeCodeMapper.GetImplementation(typeof(T)).GrainClass;

            IEnumerable<Tuple<GrainId, string, int>> activeGrainList = await GetGrainActivations();
            IEnumerable<T> filteredList = activeGrainList.Where(s => s.Item2.Equals(grainTypeName))
                .Select(s => GrainClient.GrainFactory.GetGrain<T>(s.Item1.GetPrimaryKey(), typeof(T))); //vv2 GrainClient.GrainFactory
            return filteredList.ToList();
        }

        public static async Task<IEnumerable<T>> GetActiveGrains<T>(IGrainFactory gf, params SiloAddress[] hostsIds) where T : IGrain
        {
            string grainTypeName = TypeCodeMapper.GetImplementation(typeof(T)).GrainClass;

            IEnumerable<Tuple<GrainId, string, int>> activeGrainList = await GetGrainActivations(hostsIds);
            IEnumerable<T> filteredList = activeGrainList.Where(s => s.Item2.Equals(grainTypeName)).Select(s => gf.GetGrain<T>(s.Item1.GetPrimaryKey(), typeof(T)));
            return filteredList.ToList();
        }

        private static async Task<IEnumerable<Tuple<GrainId, string, int>>> GetGrainActivations()
        {
            Dictionary<SiloAddress, SiloStatus> hosts = await SiloUtils.GetHosts(GrainClient.GrainFactory, true);
            SiloAddress[] silos = hosts.Keys.ToArray();
            return await GetGrainActivations(silos);
        }

        internal static async Task<IEnumerable<Tuple<GrainId, string, int>>> GetGrainActivations(params SiloAddress[] hostsIds)
        {
            IEnumerable<Task<List<Tuple<GrainId, string, int>>>> all = SiloUtils.GetSiloAddresses(hostsIds)
                    .Select(s => SiloUtils.GetSiloControlReference(GrainClient.GrainFactory, s).GetGrainStatistics());
            List<Tuple<GrainId, string, int>>[] result = await Task.WhenAll(all);
            return result.SelectMany(s => s);
        }
#endif
    }
}

