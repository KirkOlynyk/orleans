namespace Orleans.Indexing.Tests
{
    public interface ITestIndexState : ITestIndexProperties
    {
        string UnIndexedString { get; set; }
    }
}
