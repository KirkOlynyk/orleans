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

        internal Silo Silo {
            get {
                // Note: This must not be called until the Silo ctor has returned to the ServiceProvider which then
                // sets the Singleton; if called during the Silo ctor, the Singleton is not found so another Silo is
                // constructed. Thus we cannot have the Silo on the IndexingManager ctor params, because ISiloLifecycle
                // participants are constructed during the Silo ctor.
                this.silo = this.silo ?? this.ServiceProvider.GetRequiredService<Silo>();
                return this.silo;
            }
        }
        private Silo silo;

        internal IDictionary<Type, IDictionary<string, Tuple<object, object, object>>> Indexes { get; private set; }

        internal IServiceProvider ServiceProvider { get; private set; }

        internal ICatalog Catalog => this.Silo.Catalog;

        internal IGrainFactory GrainFactory => this.RuntimeClient.InternalGrainFactory;

        internal IGrainTypeResolver GrainTypeResolver => this.RuntimeClient.GrainTypeResolver;

        internal IRuntimeClient RuntimeClient {     // vv2err This is an internal interface
            get {
                this.runtimeClient = this.runtimeClient ?? this.ServiceProvider.GetRequiredService<IRuntimeClient>();
                return this.runtimeClient;
            }
        }
        private IRuntimeClient runtimeClient;

        internal ISiloStatusOracle SiloStatusOracle {
            get {
                this.siloStatusOracle = this.siloStatusOracle ?? this.ServiceProvider.GetRequiredService<ISiloStatusOracle>();
                return this.siloStatusOracle;
            }
        }
        private ISiloStatusOracle siloStatusOracle;

        internal ILoggerFactory LoggerFactory
        {
            get
            {
                this.loggerFactory = this.LoggerFactory ?? this.ServiceProvider.GetRequiredService<ILoggerFactory>();
                return this.LoggerFactory;
            }
        }
        private ILoggerFactory loggerFactory;

        public IndexingManager(IApplicationPartManager applicationPartManager, IServiceProvider serviceProvider)
        {
            this.ApplicationPartManager = applicationPartManager;
            this.ServiceProvider = serviceProvider;
            this.CachedTypeResolver = new CachedTypeResolver();
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
            this.Indexes = this.Indexes ?? new ApplicationPartsIndexableGrainLoader(this).GetGrainClassIndexes();
            return Task.CompletedTask;  //vv2 Load app parts and get info
        }

        /// <summary>
        /// This method is called at the begining of the process of uninitializing runtime services.
        /// </summary>
        public virtual Task OnStopAsync(CancellationToken ct)
        {
            return Task.CompletedTask;  //vv2 nothing yet
        }

        internal static IndexingManager GetIndexingManager(ref IndexingManager indexingManager, IServiceProvider serviceProvider)
        {
            indexingManager = indexingManager ?? serviceProvider.GetRequiredService<IndexingManager>();
            return indexingManager;
        }

        internal SiloAddress[] GetSiloAddresses(SiloAddress[] silos)
        {
            return (silos != null && silos.Length > 0)
                ? silos
                : this.SiloStatusOracle.GetApproximateSiloStatuses(true).Select(s => s.Key).ToArray();
        }

        internal ISiloControl GetSiloControlReference(SiloAddress siloAddress)
        {
            return this.GetSystemTarget<ISiloControl>(Constants.SiloControlId, siloAddress);
        }

        internal T GetSystemTarget<T>(GrainId grainId, SiloAddress siloAddress) where T: ISystemTarget
        {
            return this.RuntimeClient.InternalGrainFactory.GetSystemTarget<T>(grainId, siloAddress);
        }
    }
}
