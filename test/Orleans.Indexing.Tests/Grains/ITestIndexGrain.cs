using System.Threading.Tasks;

namespace Orleans.Indexing.Tests
{
    public interface ITestIndexGrain : IGrainWithIntegerKey
    {
        Task<string> GetUnIndexedString();
        Task SetUnIndexedString(string value);

        Task<int> GetUniqueInt();
        Task SetUniqueInt(int value);

        Task<string> GetUniqueString();
        Task SetUniqueString(string value);

        Task<int> GetNonUniqueInt();
        Task SetNonUniqueInt(int value);

        Task<string> GetNonUniqueString();
        Task SetNonUniqueString(string value);

        Task Deactivate(int delayMs = 0);
    }
}
