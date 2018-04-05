using System.Threading.Tasks;
using System.Collections.Generic;
using System;
using Orleans.Runtime;
using System.Linq;

namespace Orleans.Indexing
{
    /// <summary>
    /// Grain implementation class ActiveGrainEnumeratorGrain.
    /// </summary>
    public class ActiveGrainEnumeratorGrain : Grain, IActiveGrainEnumeratorGrain
    {
        internal IndexManager IndexManager => IndexManager.GetIndexManager(ref __indexManager, base.ServiceProvider);
        private IndexManager __indexManager;

        public async Task<IEnumerable<Guid>> GetActiveGrains(string grainTypeName)
        {
            IEnumerable<Tuple<GrainId, string, int>> activeGrainList = await GetGrainActivations();
            IEnumerable<Guid> filteredList = activeGrainList.Where(s => s.Item2.Equals(grainTypeName)).Select(s => s.Item1.GetPrimaryKey());
            return filteredList.ToList();
        }

        public async Task<IEnumerable<IGrain>> GetActiveGrains(Type grainType)
        {
            string grainTypeName = TypeCodeMapper.GetImplementation(this.IndexManager.RuntimeClient, grainType).GrainClass;

            IEnumerable<Tuple<GrainId, string, int>> activeGrainList = await GetGrainActivations();
            IEnumerable<IGrain> filteredList = activeGrainList.Where(s => s.Item2.Equals(grainTypeName))
                    .Select(s => GrainFactory.GetGrain<IIndexableGrain>(this.IndexManager.GrainTypeResolver, s.Item1.GetPrimaryKey(), grainType));
            return filteredList.ToList();
        }

        private async Task<IEnumerable<Tuple<GrainId, string, int>>> GetGrainActivations()
        {
            Dictionary<SiloAddress, SiloStatus> hosts = await this.IndexManager.GetSiloHosts(true);
            return await GetGrainActivations(hosts.Keys.ToArray());
        }

        private async Task<IEnumerable<Tuple<GrainId, string, int>>> GetGrainActivations(SiloAddress[] hostsIds)
        {
            IEnumerable<Task<List<Tuple<GrainId, string, int>>>> all = this.IndexManager.GetSiloAddresses(hostsIds)
                    .Select(s => this.IndexManager.GetSiloControlReference(s).GetGrainStatistics());
            List<Tuple<GrainId, string, int>>[] result = await Task.WhenAll(all);
            return result.SelectMany(s => s);
        }
    }
}
