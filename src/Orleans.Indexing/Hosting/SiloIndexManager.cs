using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.ApplicationParts;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.Indexing
{
    /// <summary>
    /// This class is instantiated internally only in the Silo.
    /// </summary>
    class SiloIndexManager : IndexManager, ILifecycleParticipant<ISiloLifecycle>
    {
        internal SiloAddress SiloAddress => this.Silo.SiloAddress;

        internal ICatalog Catalog => this.Silo.Catalog;

        // Note: this.Silo must not be called until the Silo ctor has returned to the ServiceProvider which then
        // sets the Singleton; if called during the Silo ctor, the Singleton is not found so another Silo is
        // constructed. Thus we cannot have the Silo on the IndexManager ctor params or retrieve it during
        // IndexManager ctor, because ISiloLifecycle participants are constructed during the Silo ctor.
        internal Silo Silo => __silo ?? (__silo = this.ServiceProvider.GetRequiredService<Silo>());
        private Silo __silo;

        internal ISiloRuntimeClient SiloRuntimeClient { get; }

        internal ISiloStatusOracle SiloStatusOracle { get; }

        public SiloIndexManager(ISiloRuntimeClient src, ISiloStatusOracle sso,
                                IServiceProvider sp, IGrainFactory gf, IApplicationPartManager apm, ILoggerFactory lf)
            : base(sp, gf, apm, lf)
        {
            this.SiloRuntimeClient = src;
            this.SiloStatusOracle = sso;
        }

        public void Participate(ISiloLifecycle lifecycle)
        {
            lifecycle.Subscribe(this.GetType().FullName, ServiceLifecycleStage.RuntimeGrainServices, ct => base.OnStartAsync(ct), ct => base.OnStopAsync(ct));
        }

        internal Task<Dictionary<SiloAddress, SiloStatus>> GetSiloHosts(bool onlyActive = false)
            => this.GrainFactory.GetGrain<IManagementGrain>(0).GetHosts(onlyActive);

        internal SiloAddress[] GetSiloAddresses(SiloAddress[] silos)
            => (silos != null && silos.Length > 0)
                ? silos
                : this.SiloStatusOracle.GetApproximateSiloStatuses(true).Select(s => s.Key).ToArray();

        internal ISiloControl GetSiloControlReference(SiloAddress siloAddress)
            => this.GetSystemTarget<ISiloControl>(Constants.SiloControlId, siloAddress);

        internal T GetSystemTarget<T>(GrainId grainId, SiloAddress siloAddress) where T : ISystemTarget
            => this.RuntimeClient.InternalGrainFactory.GetSystemTarget<T>(grainId, siloAddress);
    }
}
