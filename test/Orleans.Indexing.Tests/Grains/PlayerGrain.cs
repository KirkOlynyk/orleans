using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Providers;

namespace Orleans.Indexing.Tests
{
    /// <summary>
    /// A simple grain that represent a player in a game
    /// </summary>
    [StorageProvider(ProviderName = "MemoryStore")]
    public abstract class PlayerGrain<TState, TProps> : IndexableGrain<TState, TProps>, IPlayerGrain where TState : IPlayerState where TProps : new()
    {
        protected ILogger Logger { get; private set; }
        
        public string Email => this.State.Email;
        public string Location => this.State.Location;
        public int Score => this.State.Score;

        public override Task OnActivateAsync()
        {
            this.Logger = this.ServiceProvider.GetRequiredService<ILoggerFactory>().CreateLogger("PlayerGrain-" + this.IdentityString);
            return base.OnActivateAsync();
        }

        public Task<string> GetLocation() => Task.FromResult(this.Location);

        public async Task SetLocation(string location)
        {
            int counter = 0;
            while (true)
            {
                base.State.Location = location;
                //return Task.CompletedTask;
                try
                {
                    await base.WriteStateAsync();
                    return;
                }
                catch(Exception e)
                {
                    if (counter > 10) throw e;
                    ++counter;
                    await base.ReadStateAsync();
                }
            }
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
