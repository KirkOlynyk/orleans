using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.ApplicationParts;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Indexing
{
    /// <summary>
    /// This class may be instantiated internally in the ClusterClient as well as in the Silo.
    /// </summary>
    internal class IndexManager : ILifecycleParticipant<IClusterClientLifecycle>
    {
        internal IApplicationPartManager ApplicationPartManager;

        internal CachedTypeResolver CachedTypeResolver { get; }

        internal IDictionary<Type, IDictionary<string, Tuple<object, object, object>>> Indexes { get; private set; }    // vv2 TODO strongly type this

        // Explicit dependency on ServiceProvider is needed so we can retrieve SiloIndexManager.__silo after ctor returns; see comments there.
        // Also, in some cases this is passed through non-injected interfaces such as Hash classes.
        internal IServiceProvider ServiceProvider { get; }

        internal IGrainFactory GrainFactory { get; }

        internal IGrainTypeResolver GrainTypeResolver => this.RuntimeClient.GrainTypeResolver;

        // Note: For similar reasons as SiloIndexManager.__silo, __indexFactory relies on 'this' to have returned from its ctor.
        internal IndexFactory IndexFactory => this.__indexFactory ?? (__indexFactory = this.ServiceProvider.GetRequiredService<IndexFactory>());
        private IndexFactory __indexFactory;

        internal IRuntimeClient RuntimeClient { get; }

        internal ILoggerFactory LoggerFactory { get; }

        public IndexManager(IServiceProvider sp, IGrainFactory gf, IApplicationPartManager apm, ILoggerFactory lf)
        {
            this.ServiceProvider = sp;
            this.RuntimeClient = sp.GetRequiredService<IRuntimeClient>();
            this.GrainFactory = gf;
            this.ApplicationPartManager = apm;
            this.LoggerFactory = lf;
            this.CachedTypeResolver = new CachedTypeResolver();
        }

        public void Participate(IClusterClientLifecycle lifecycle)
        {
            if (!(this is SiloIndexManager))
            {
                lifecycle.Subscribe(this.GetType().FullName, ServiceLifecycleStage.RuntimeGrainServices, ct => this.OnStartAsync(ct), ct => this.OnStopAsync(ct));
            }
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
        public virtual Task OnStopAsync(CancellationToken ct) => Task.CompletedTask;

        internal static IndexManager GetIndexManager(ref IndexManager indexManager, IServiceProvider serviceProvider)
            => indexManager ?? (indexManager = GetIndexManager(serviceProvider));

        internal static IndexManager GetIndexManager(IServiceProvider serviceProvider)
            => serviceProvider.GetRequiredService<IndexManager>();

        internal static SiloIndexManager GetSiloIndexManager(ref SiloIndexManager siloIndexManager, IServiceProvider serviceProvider)
            => siloIndexManager ?? (siloIndexManager = GetSiloIndexManager(serviceProvider));

        internal static SiloIndexManager GetSiloIndexManager(IServiceProvider serviceProvider)
            => (SiloIndexManager)serviceProvider.GetRequiredService<IndexManager>();    // Throws an invalid cast operation if we're not on a Silo
    }
}
