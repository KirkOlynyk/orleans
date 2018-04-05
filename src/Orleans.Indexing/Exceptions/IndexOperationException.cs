using System;

namespace Orleans.Indexing
{
    /// <summary>
    /// This exception is thrown when an indexing operation exception is encountered.
    /// </summary>
    public class IndexOperationException : IndexException
    {
        public IndexOperationException(string message) : base(message)
        {
        }
    }
}
