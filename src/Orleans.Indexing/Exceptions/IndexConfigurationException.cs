namespace Orleans.Indexing
{
    /// <summary>
    /// This exception is thrown when an indexing configuration exception is encountered.
    /// </summary>
    class IndexConfigurationException : IndexException
    {
        public IndexConfigurationException(string message) : base(message)
        {
        }
    }
}
