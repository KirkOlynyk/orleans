using System;

namespace Orleans.Indexing
{
    /// <summary>
    /// This class is a wrapper around another IMemberUpdate, which overrides
    /// the actual operation in the original update
    /// </summary>
    [Serializable]
    public class MemberUpdateOverridenOperation : IMemberUpdate
    {
        private IMemberUpdate _update;
        private IndexOperationType _opType;
        public MemberUpdateOverridenOperation(IMemberUpdate update, IndexOperationType opType)
        {
            this._update = update;
            this._opType = opType;
        }
        public object GetBeforeImage()
        {
            return (this._opType == IndexOperationType.Update || this._opType == IndexOperationType.Delete) ? this._update.GetBeforeImage() : null;
        }

        public object GetAfterImage()
        {
            return (this._opType == IndexOperationType.Update || this._opType == IndexOperationType.Insert) ? this._update.GetAfterImage() : null;
        }

        public IndexOperationType GetOperationType()
        {
            return this._opType;
        }

        public override string ToString()
        {
            return MemberUpdate.ToString(this);
        }
    }
}
