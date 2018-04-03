using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.ApplicationParts;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Indexing
{
    class IndexingManager : ILifecycleParticipant<ISiloLifecycle>
    {
        internal IApplicationPartManager ApplicationPartManager;

        internal CachedTypeResolver CachedTypeResolver { get; private set; }

        internal SiloAddress SiloAddress => this.Silo.SiloAddress;

        internal IDictionary<Type, IDictionary<string, Tuple<object, object, object>>> Indexes { get; private set; }

        // Explicit dependency on ServiceProvider is needed so we can retrieve Silo after ctor returns; see comments on Silo property.
        // Also, in some cases this is passed through non-injected interfaces such as Hash classes.
        internal IServiceProvider ServiceProvider { get; private set; }

        internal ICatalog Catalog => this.Silo.Catalog;

        internal IGrainFactory GrainFactory => this.RuntimeClient.InternalGrainFactory;

        internal IGrainTypeResolver GrainTypeResolver => this.RuntimeClient.GrainTypeResolver;

        // Note: this.Silo must not be called until the Silo ctor has returned to the ServiceProvider which then
        // sets the Singleton; if called during the Silo ctor, the Singleton is not found so another Silo is
        // constructed. Thus we cannot have the Silo on the IndexingManager ctor params or retrieve it during
        // IndexingManager ctor, because ISiloLifecycle participants are constructed during the Silo ctor.
        internal Silo Silo => this.__silo ?? (this.__silo = this.ServiceProvider.GetRequiredService<Silo>());
        private Silo __silo;

        internal IRuntimeClient RuntimeClient { get; private set; }

        internal ISiloStatusOracle SiloStatusOracle { get; private set; }

        internal ILoggerFactory LoggerFactory { get; private set; }

        internal IndexFactory IndexFactory { get; private set; }

        public IndexingManager(IServiceProvider sp, IApplicationPartManager apm, IRuntimeClient rc, ISiloStatusOracle sso, ILoggerFactory lf)
        {
            this.ServiceProvider = sp;
            this.ApplicationPartManager = apm;
            this.RuntimeClient = rc;
            this.SiloStatusOracle = sso;
            this.LoggerFactory = lf;
            this.CachedTypeResolver = new CachedTypeResolver();
            this.IndexFactory = new IndexFactory(this, this.GrainFactory, this.RuntimeClient);  // vv2 this vs. singleton: is singleton ever used?
        }

        public void Participate(ISiloLifecycle lifecycle)
        {
            lifecycle.Subscribe(SiloLifecycleStage.RuntimeGrainServices, ct => OnStartAsync(ct), ct => OnStopAsync(ct));
        }

        /// <summary>
        /// This method is called after runtime services have been initialized; all application parts have been loaded.
        /// </summary>
        public virtual Task OnStartAsync(CancellationToken ct)
        {
            return (this.Indexes == null)
                ? Task.Run(() => this.Indexes = new ApplicationPartsIndexableGrainLoader(this).GetGrainClassIndexes())
                : Task.CompletedTask;
        }

        /// <summary>
        /// This method is called at the begining of the process of uninitializing runtime services.
        /// </summary>
        public virtual Task OnStopAsync(CancellationToken ct)
        {
            return Task.CompletedTask;  //vv2 nothing yet
        }

        internal static IndexingManager GetIndexingManager(ref IndexingManager indexingManager, IServiceProvider serviceProvider)
            => indexingManager ?? (indexingManager = GetIndexingManager(serviceProvider));

        internal static IndexingManager GetIndexingManager(IServiceProvider serviceProvider)
            => serviceProvider.GetRequiredService<IndexingManager>();

        internal SiloAddress[] GetSiloAddresses(SiloAddress[] silos)
            => (silos != null && silos.Length > 0)
                ? silos
                : this.SiloStatusOracle.GetApproximateSiloStatuses(true).Select(s => s.Key).ToArray();

        internal ISiloControl GetSiloControlReference(SiloAddress siloAddress)
            => this.GetSystemTarget<ISiloControl>(Constants.SiloControlId, siloAddress);

        internal T GetSystemTarget<T>(GrainId grainId, SiloAddress siloAddress) where T: ISystemTarget
            => this.RuntimeClient.InternalGrainFactory.GetSystemTarget<T>(grainId, siloAddress);
    }
}
