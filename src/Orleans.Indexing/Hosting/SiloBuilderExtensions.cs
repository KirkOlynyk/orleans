using System;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Runtime;

namespace Orleans.Indexing
{
    public static class SiloBuilderExtensions
    {
        /// <summary>
        /// Configure silo to use indexing using a configure action.
        /// </summary>
        public static ISiloHostBuilder UseIndexing(this ISiloHostBuilder builder, Action<IndexingOptions> configureOptions)
        {
            return builder.ConfigureServices(services => services.UseIndexing(ob => ob.Configure(configureOptions)))
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(SiloBuilderExtensions).Assembly));
        }

        /// <summary>
        /// Configure silo to use indexing using a configuration builder.
        /// </summary>
        public static ISiloHostBuilder UseIndexing(this ISiloHostBuilder builder, Action<OptionsBuilder<IndexingOptions>> configureAction = null)
        {
            return builder.ConfigureServices(services => services.UseIndexing(configureAction))
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(SiloBuilderExtensions).Assembly));
        }

        /// <summary>
        /// Configure silo services to use indexing using a configuration builder.
        /// </summary>
        private static IServiceCollection UseIndexing(this IServiceCollection services, Action<OptionsBuilder<IndexingOptions>> configureAction = null)
        {
            configureAction?.Invoke(services.AddOptions<IndexingOptions>(IndexingConstants.INDEXING_OPTIONS_NAME));
            services.AddSingleton<IndexFactory>()
                    .AddFromExisting<IIndexFactory, IndexFactory>();
            services.AddSingleton<SiloIndexManager>()
                    .AddFromExisting<ILifecycleParticipant<ISiloLifecycle>, SiloIndexManager>();
            services.AddFromExisting<IndexManager, SiloIndexManager>();
            return services;
        }
    }
}
