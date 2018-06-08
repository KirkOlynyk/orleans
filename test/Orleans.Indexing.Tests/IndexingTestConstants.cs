namespace Orleans.Indexing.Tests
{
    public static class IndexingTestConstants
    {
        // storage providers
        public const string GrainStore = "GrainStore";
        public const string MemoryStore = "MemoryStore";

        public const string Seattle = "Seattle";
        public const string SanFrancisco = "San Francisco";
        public const string SanJose = "SanJose";
        public const string Bellevue = "Bellevue";
        public const string Redmond = "Redmond";
        public const string Kirkland = "Kirkland";
        public const string Tehran = "Tehran";
        public const string Yazd = "Yazd";

        public const string LocationIndex = "__Location";
        public const string UniqueIntIndex = "__UniqueInt";
        public const string UniqueStringIndex = "__UniqueString";
        public const string NonUniqueIntIndex = "__NonUniqueInt";
        public const string NonUniqueStringIndex = "__NonUniqueString";

        public const int DelayUntilIndexesAreUpdatedLazily = 1000; // One-second delay for writes to the in-memory indexes should be enough
    }
}
