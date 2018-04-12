using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using System;

namespace Orleans.Indexing
{
    public static class ClientBuilderExtensions
    {
        /// <summary>
        /// Configure cluster to use indexing using a configure action.
        /// </summary>
        public static IClientBuilder UseIndexing(this IClientBuilder builder, Action<IndexingOptions> configureOptions)
        {
            return builder.ConfigureServices(services => services.UseIndexing(ob => ob.Configure(configureOptions)))
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(SiloBuilderExtensions).Assembly));
        }

        /// <summary>
        /// Configure cluster to use indexing using a configuration builder.
        /// </summary>
        public static IClientBuilder UseIndexing(this IClientBuilder builder, Action<OptionsBuilder<IndexingOptions>> configureAction = null)
        {
            return builder.ConfigureServices(services => services.UseIndexing(configureAction))
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(SiloBuilderExtensions).Assembly));
        }

        /// <summary>
        /// Configure cluster services to use indexing using a configuration builder.
        /// </summary>
        private static IServiceCollection UseIndexing(this IServiceCollection services, Action<OptionsBuilder<IndexingOptions>> configureAction = null)
        {
            configureAction?.Invoke(services.AddOptions<IndexingOptions>());
            services.AddSingleton<IndexFactory>()
                    .AddFromExisting<IIndexFactory, IndexFactory>();
            services.AddSingleton<IndexManager>()
                    .AddFromExisting<ILifecycleParticipant<IClusterClientLifecycle>, IndexManager>();
            return services;
        }
    }
}
