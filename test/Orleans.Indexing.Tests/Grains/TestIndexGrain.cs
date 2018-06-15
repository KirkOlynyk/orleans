using Orleans.Providers;
using System.Threading.Tasks;

namespace Orleans.Indexing.Tests
{
    [StorageProvider(ProviderName = IndexingConstants.MEMORY_STORAGE_PROVIDER_NAME)]
    public abstract class TestIndexGrain<TState, TProps> : IndexableGrain<TState, TProps>, ITestIndexGrain
        where TState : ITestIndexState where TProps : ITestIndexProperties, new()
    {
        private TestIndexGrainBase<TState, TProps> testBase;

        public string UnIndexedString => this.State.UnIndexedString;
        public Task<string> GetUnIndexedString() => Task.FromResult(this.State.UnIndexedString);
        public Task SetUnIndexedString(string value) => testBase.SetProperty(v => this.State.UnIndexedString = v, value, retry:false);

        public int UniqueInt => this.State.UniqueInt;
        public Task<int> GetUniqueInt() => Task.FromResult(this.State.UniqueInt);
        public Task SetUniqueInt(int value) => testBase.SetProperty(v => this.State.UniqueInt = v, value, retry:testBase.IsUniqueIntIndexed);
        public Task SetUniqueIntWithoutWrite(int value) => testBase.SetPropertyWithoutWrite(v => this.State.UniqueInt = v, value);

        public string UniqueString => base.State.UniqueString;
        public Task<string> GetUniqueString() => Task.FromResult(this.State.UniqueString);
        public Task SetUniqueString(string value) => testBase.SetProperty(v => this.State.UniqueString = v, value, retry:testBase.IsUniqueStringIndexed);
        public Task SetUniqueStringWithoutWrite(string value) => testBase.SetPropertyWithoutWrite(v => this.State.UniqueString = v, value);

        public int NonUniqueInt => this.State.NonUniqueInt;
        public Task<int> GetNonUniqueInt() => Task.FromResult(this.State.NonUniqueInt);
        public Task SetNonUniqueInt(int value) => testBase.SetProperty(v => this.State.NonUniqueInt = v, value, retry:testBase.IsNonUniqueIntIndexed);
        public Task SetNonUniqueIntWithoutWrite(int value) => testBase.SetPropertyWithoutWrite(v => this.State.NonUniqueInt = v, value);

        public string NonUniqueString => base.State.NonUniqueString;
        public Task<string> GetNonUniqueString() => Task.FromResult(this.State.NonUniqueString);
        public Task SetNonUniqueString(string value) => testBase.SetProperty(v => this.State.NonUniqueString = v, value, retry:testBase.IsNonUniqueStringIndexed);
        public Task SetNonUniqueStringWithoutWrite(string value) => testBase.SetPropertyWithoutWrite(v => this.State.NonUniqueString = v, value);

        public Task Deactivate()
        {
            base.DeactivateOnIdle();
            return Task.CompletedTask;
        }

        public TestIndexGrain()
        {
            this.testBase = new TestIndexGrainBase<TState, TProps>(() => base.WriteStateAsync(), () => base.ReadStateAsync());
        }
    }
}
