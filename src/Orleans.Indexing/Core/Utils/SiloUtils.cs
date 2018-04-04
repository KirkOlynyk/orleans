using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.Indexing
{
    /// <summary>
    /// A utility class for the low-level operations related to silos
    /// </summary>
    internal static class SiloUtils
    {
#region copy & paste from ManagementGrain.cs    // vv2 clean up to direct callthrough to IndexManager if no longer needed
        internal static Task<Dictionary<SiloAddress, SiloStatus>> GetHosts(IGrainFactory grainFactory, bool onlyActive = false)
        {
            var mgmtGrain = grainFactory.GetGrain<IManagementGrain>(0);
            return mgmtGrain.GetHosts(onlyActive);
        }

        internal static SiloAddress[] GetSiloAddresses(IndexManager indexManager, SiloAddress[] silos)
        {
            return indexManager.GetSiloAddresses(silos);
        }

        internal static ISiloControl GetSiloControlReference(IndexManager indexManager, SiloAddress silo)
        {
            return indexManager.GetSiloControlReference(silo);
        }

#endregion
    }
}
