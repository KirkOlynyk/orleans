using System;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Xunit;

namespace Orleans.Indexing.Tests
{
    internal class TestIndexGrainBase<TState, TProps> where TProps: new()
    {
        internal bool IsUniqueIntIndexed;
        internal bool IsUniqueStringIndexed;
        internal bool IsNonUniqueIntIndexed;
        internal bool IsNonUniqueStringIndexed;

        private Func<Task> writeStateFunc;
        private Func<Task> readStateFunc;

        internal TestIndexGrainBase(Func<Task> wsf, Func<Task> rsf)
        {
            this.writeStateFunc = wsf;
            this.readStateFunc = rsf;

            Type propertiesType = typeof(TProps);
            Assert.True(propertiesType.IsClass);

            bool isIndexed(string propertyName)
            {
                var propInfo = propertiesType.GetProperty(propertyName);
                return propInfo.GetCustomAttributes<IndexAttribute>(inherit: false).Any();
            }

            this.IsUniqueIntIndexed = isIndexed(nameof(ITestIndexProperties.UniqueInt));
            this.IsUniqueStringIndexed = isIndexed(nameof(ITestIndexProperties.UniqueString));
            this.IsNonUniqueIntIndexed = isIndexed(nameof(ITestIndexProperties.NonUniqueInt));
            this.IsNonUniqueStringIndexed = isIndexed(nameof(ITestIndexProperties.NonUniqueString));

            Assert.True(IsUniqueIntIndexed || IsUniqueStringIndexed || IsNonUniqueIntIndexed || IsNonUniqueStringIndexed);
        }

        internal async Task SetProperty<T>(Action<T> setter, T value, bool retry)
        {
            const int MaxRetries = 10;
            int retries = 0;
            while (true)
            {
                setter(value);
                try
                {
                    await writeStateFunc();
                    return;
                }
                catch (Exception)
                {
                    if (!retry || ++retries >= MaxRetries) throw;
                    await readStateFunc();
                }
            }
        }

        internal Task SetPropertyWithoutWrite<T>(Action<T> setter, T value)
        {
            setter(value);
            return Task.CompletedTask;
        }
    }
}
