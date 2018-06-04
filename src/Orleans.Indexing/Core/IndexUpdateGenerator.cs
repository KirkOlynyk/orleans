using System;
using System.Reflection;

namespace Orleans.Indexing
{
    /// <summary>
    /// 
    /// </summary>
    [Serializable]
    internal class IndexUpdateGenerator : IIndexUpdateGenerator
    {
        PropertyInfo _prop;
        public IndexUpdateGenerator(PropertyInfo prop)
            => this._prop = prop;

        public IMemberUpdate CreateMemberUpdate(object gProps, object befImg)
        {
            object aftImg = gProps == null ? null : ExtractIndexImage(gProps);
            return new MemberUpdate(befImg, aftImg);
        }

        public IMemberUpdate CreateMemberUpdate(object aftImg)
            => new MemberUpdate(null, aftImg);

        public object ExtractIndexImage(object gProps)
            => this._prop.GetValue(gProps);
    }
}
