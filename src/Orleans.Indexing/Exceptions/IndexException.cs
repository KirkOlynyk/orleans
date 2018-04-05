using Orleans.Runtime;

namespace Orleans.Indexing
{
    /// <summary>
    /// This exception is thrown when a general indexing exception is encountered, or as a base for more specific subclasses.
    /// </summary>
    public class IndexException : OrleansException
    {
        public IndexException(string message) : base(message)
        {
        }
    }
}
