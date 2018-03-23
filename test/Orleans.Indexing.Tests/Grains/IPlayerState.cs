namespace Orleans.Indexing.Tests
{
    public interface IPlayerState : IPlayerProperties
    {
        string Email { get; set; }
    }
}
