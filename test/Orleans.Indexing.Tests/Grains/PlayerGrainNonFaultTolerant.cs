using System;
using System.Threading.Tasks;
using Orleans;
using Orleans.Providers;
using Orleans.Runtime;
using Orleans.Indexing;

namespace Orleans.Indexing.Tests
{
    /// <summary>
    /// A simple grain that represent a player in a game
    /// </summary>
    [StorageProvider(ProviderName = "MemoryStore")]
    public abstract class PlayerGrainNonFaultTolerant<TState, TProps> : IndexableGrainNonFaultTolerant<TState, TProps>, IPlayerGrain where TState : IPlayerState, new() where TProps : new()
    {
        private object logger; //vv2 Logger logger;
        
        public string Email { get { return this.State.Email; } }
        public string Location { get { return this.State.Location; } }
        public int Score { get { return this.State.Score; } }

        public override Task OnActivateAsync()
        {
            this.logger = this.GetLogger("PlayerGrainNonFaultTolerant-" + this.IdentityString);
            return base.OnActivateAsync();
        }

        public Task<string> GetLocation()
        {
            return Task.FromResult(this.Location);
        }

        public Task SetLocation(string location)
        {
            this.State.Location = location;
            //return TaskDone.Done;
            return base.WriteStateAsync();
        }

        public Task<int> GetScore()
        {
            return Task.FromResult(this.Score);
        }

        public Task SetScore(int score)
        {
            this.State.Score = score;
            //return TaskDone.Done;
            return base.WriteStateAsync();
        }

        public Task<string> GetEmail()
        {
            return Task.FromResult(this.Email);
        }

        public Task SetEmail(string email)
        {
            this.State.Email = email;
            //return TaskDone.Done;
            return base.WriteStateAsync();
        }

        public Task Deactivate()
        {
            DeactivateOnIdle();
            return Task.CompletedTask;
        }
    }
}
