using System;
using System.Collections.Generic;

namespace Orleans.Indexing
{
    class IndexRegistry
    {
        private IDictionary<Type, NamedIndexMap> IndexRegistriesByGrainType { get; set; } = new Dictionary<Type, NamedIndexMap>();

        internal NamedIndexMap this[Type grainType]
        {
            get => this.IndexRegistriesByGrainType[grainType];
            set => this.IndexRegistriesByGrainType[grainType] = value;
        }

        internal bool TryGetValue(Type grainType, out NamedIndexMap grainIndexes)
            => this.IndexRegistriesByGrainType.TryGetValue(grainType, out grainIndexes);

        internal bool ContainsKey(Type grainType) => this.IndexRegistriesByGrainType.ContainsKey(grainType);
    }
}
