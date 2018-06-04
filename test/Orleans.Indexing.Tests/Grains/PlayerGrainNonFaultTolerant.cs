using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Providers;

namespace Orleans.Indexing.Tests
{
    /// <summary>
    /// A simple grain that represents a player in a game
    /// </summary>
    [StorageProvider(ProviderName = IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
    public abstract class PlayerGrainNonFaultTolerant<TState, TProps> : IndexableGrainNonFaultTolerant<TState, TProps>, IPlayerGrain where TState : IPlayerState, new() where TProps : new()
    {
        protected ILogger Logger { get; private set; }

        public string Email => this.State.Email;
        public string Location => this.State.Location;
        public int Score => this.State.Score;

        public override Task OnActivateAsync()
        {
            this.Logger = this.ServiceProvider.GetRequiredService<ILoggerFactory>().CreateLogger("PlayerGrainNonFaultTolerant-" + this.IdentityString);
            return base.OnActivateAsync();
        }

        public Task<string> GetLocation() => Task.FromResult(this.Location);

        public Task SetLocation(string location)
        {
            this.State.Location = location;
            //return Task.CompletedTask;
            return base.WriteStateAsync();
        }

        public Task<int> GetScore() => Task.FromResult(this.Score);

        public Task SetScore(int score)
        {
            this.State.Score = score;
            //return Task.CompletedTask;
            return base.WriteStateAsync();
        }

        public Task<string> GetEmail() => Task.FromResult(this.Email);

        public Task SetEmail(string email)
        {
            this.State.Email = email;
            //return Task.CompletedTask;
            return base.WriteStateAsync();
        }

        public Task Deactivate()
        {
            DeactivateOnIdle();
            return Task.CompletedTask;
        }
    }
}
