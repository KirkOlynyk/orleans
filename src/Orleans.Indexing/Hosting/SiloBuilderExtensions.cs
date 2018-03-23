using System;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Runtime;

namespace Orleans.Indexing
{
    public static class SiloBuilderExtensions
    {
        /// <summary>
        /// Configure cluster to use indexing using a configure action.
        /// </summary>
        public static ISiloHostBuilder UseIndexing(this ISiloHostBuilder builder, Action<IndexingOptions> configureOptions)
        {
            return builder.ConfigureServices(services => services.UseIndexing(ob => ob.Configure(configureOptions)))
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(SiloBuilderExtensions).Assembly));
        }

        /// <summary>
        /// Configure cluster to use indexing using a configuration builder.
        /// </summary>
        public static ISiloHostBuilder UseIndexing(this ISiloHostBuilder builder, Action<OptionsBuilder<IndexingOptions>> configureOptions = null)
        {
            return builder.ConfigureServices(services => services.UseIndexing(configureOptions))
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(SiloBuilderExtensions).Assembly));
        }

        /// <summary>
        /// Configure cluster services to use indexing using a configuration builder.
        /// </summary>
        private static IServiceCollection UseIndexing(this IServiceCollection services,
        Action<OptionsBuilder<IndexingOptions>> configureOptions = null)
        {
            configureOptions?.Invoke(services.AddOptions<IndexingOptions>());
            return services.AddSingleton<ILifecycleParticipant<ISiloLifecycle>, IndexingManager>();
        }
    }
}
