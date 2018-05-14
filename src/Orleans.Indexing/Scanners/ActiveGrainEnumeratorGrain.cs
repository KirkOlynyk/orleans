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
        internal SiloIndexManager SiloIndexManager => IndexManager.GetSiloIndexManager(ref __siloIndexManager, base.ServiceProvider);
        private SiloIndexManager __siloIndexManager;

        public async Task<IEnumerable<Guid>> GetActiveGrains(string grainTypeName)
        {
            IEnumerable<Tuple<GrainReference, string, int>> activeGrainList = await GetGrainActivations();
            IEnumerable<Guid> filteredList = activeGrainList.Where(s => s.Item2.Equals(grainTypeName)).Select(s => s.Item1.GetPrimaryKey());
            return filteredList.ToList();
        }

        public async Task<IEnumerable<IGrain>> GetActiveGrains(Type grainType)
        {
            string grainTypeName = TypeCodeMapper.GetImplementation(this.SiloIndexManager.GrainTypeResolver, grainType).GrainClass;

            IEnumerable<Tuple<GrainReference, string, int>> activeGrainList = await GetGrainActivations();
            IEnumerable<IGrain> filteredList = activeGrainList.Where(s => s.Item2.Equals(grainTypeName))
                    .Select(s => this.SiloIndexManager.GetGrain<IIndexableGrain>(s.Item1.GetPrimaryKey(), grainType));
            return filteredList.ToList();
        }

        private async Task<IEnumerable<Tuple<GrainReference, string, int>>> GetGrainActivations()
        {
            Dictionary<SiloAddress, SiloStatus> hosts = await this.SiloIndexManager.GetSiloHosts(true);
            return await this.SiloIndexManager.Silo.GetClusterGrainActivations(hosts.Keys.ToArray());
        }
    }
}
