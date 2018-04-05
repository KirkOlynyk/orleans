namespace Orleans.Indexing
{
    /// <summary>
    /// This exception is thrown when a workflow indexing exception is encountered.
    /// </summary>
    public class WorkflowIndexException : IndexException
    {
        public WorkflowIndexException(string message) : base(message)
        {
        }
    }
}
