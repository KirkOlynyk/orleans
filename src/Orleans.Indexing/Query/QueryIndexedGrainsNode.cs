using System;
using System.Threading.Tasks;
using Orleans.Streams;

namespace Orleans.Indexing
{
    /// <summary>
    /// Implements <see cref="IOrleansQueryable"/>
    /// </summary>
    public class QueryIndexedGrainsNode<TIGrain, TProperties> : QueryGrainsNode<TIGrain, TProperties> where TIGrain : IIndexableGrain
    {
        private string _indexName;

        private object _param;

        public QueryIndexedGrainsNode(IIndexFactory indexFactory, IStreamProvider streamProvider, string indexName, object param) : base(indexFactory, streamProvider)
        {
            this._indexName = indexName;
            this._param = param;
        }

        public override async Task<IOrleansQueryResult<TIGrain>> GetResults()
        {
            IIndexInterface index = base.IndexFactory.GetIndex(typeof(TIGrain), this._indexName);

            //the actual lookup for the query result to be streamed to the observer
            return (IOrleansQueryResult<TIGrain>)await index.Lookup(this._param);
        }

        public override async Task ObserveResults(IAsyncBatchObserver<TIGrain> observer)
        {
            IIndexInterface index = base.IndexFactory.GetIndex(typeof(TIGrain), this._indexName);
            IAsyncStream<TIGrain> resultStream = base.StreamProvider.GetStream<TIGrain>(Guid.NewGuid(), IndexUtils.GetIndexGrainID(typeof(TIGrain), this._indexName));

            IOrleansQueryResultStream<TIGrain> result = new OrleansQueryResultStream<TIGrain>(resultStream);

            //the observer is attached to the query result
            await result.SubscribeAsync(observer);

            //the actual lookup for the query result to be streamed to the observer
            await index.Lookup(result.Cast<IIndexableGrain>(), this._param);
        }
    }
}
