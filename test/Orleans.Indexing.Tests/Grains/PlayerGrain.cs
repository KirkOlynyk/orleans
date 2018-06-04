using System;
using System.Threading.Tasks;
using Orleans.Providers;

namespace Orleans.Indexing.Tests
{
    /// <summary>
    /// A simple grain that represent a player in a game
    /// </summary>
    [StorageProvider(ProviderName = IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
    public abstract class PlayerGrain<TState, TProps> : IndexableGrain<TState, TProps>, IPlayerGrain where TState : IPlayerState where TProps : new()
    {
        public string Email => this.State.Email;
        public string Location => this.State.Location;
        public int Score => this.State.Score;

        public Task<string> GetLocation() => Task.FromResult(this.Location);

        public async Task SetLocation(string location)
        {
            const int MaxRetries = 10;
            int retries = 0;
            while (true)
            {
                base.State.Location = location;
                try
                {
                    await base.WriteStateAsync();
                    return;
                }
                catch (Exception)
                {
                    if (++retries >= MaxRetries) throw;
                    await base.ReadStateAsync();
                }
            }
        }

        public Task<int> GetScore() => Task.FromResult(this.Score);

        public Task SetScore(int score)
        {
            this.State.Score = score;
            return base.WriteStateAsync();
        }

        public Task<string> GetEmail() => Task.FromResult(this.Email);

        public Task SetEmail(string email)
        {
            this.State.Email = email;
            return base.WriteStateAsync();
        }

        public Task Deactivate()
        {
            DeactivateOnIdle();
            return Task.CompletedTask;
        }
    }
}
