// Naming convention used when creating sub-interfaces and classes:
//  UI, US, NI, NS  - property abbrev
//  AI, TI - active or total
//  UQ, NU - Unique, NonUnique
//  EG, LZ - eager or lazy
//  PK, PS, SB - partition per key/silo or single bucket
//  FT, NFT - Fault Tolerant or Non FT
//
//  [I] Grain|Props|State_<properties>_<index_type>_<eg or lz>_<partition>[__<same>[...]]_<ft or nft>

namespace Orleans.Indexing.Tests
{
    public interface ITestIndexProperties
    {
        int UniqueInt { get; set; }

        string UniqueString { get; set; }

        int NonUniqueInt { get; set; }

        string NonUniqueString { get; set; }
    }
}
