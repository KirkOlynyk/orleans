using System;

namespace Orleans.Indexing
{
    /// <summary>
    /// This class is a wrapper around another IMemberUpdate, which represents a
    /// tentative update to an index, which should be specially taken care of by index
    /// so that the change is not visible by others, but still blocks further violation
    /// of constraints such as uniqueness constraint
    /// </summary>
    [Serializable]
    internal class MemberUpdateTentative : IMemberUpdate
    {
        private IMemberUpdate _update;

        public MemberUpdateTentative(IMemberUpdate update) => this._update = update;

        public object GetBeforeImage() => this._update.GetBeforeImage();

        public object GetAfterImage() => this._update.GetAfterImage();

        public IndexOperationType OperationType => this._update.OperationType;

        public override string ToString()
        {
            return MemberUpdate.ToString(this);
        }
    }
}
