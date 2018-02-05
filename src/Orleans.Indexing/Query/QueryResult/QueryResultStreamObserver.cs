using Orleans.Streams;
using System;
using System.Threading.Tasks;

namespace Orleans.Indexing
{
    /// <summary>
    /// This class is used for creating IAsyncBatchObserver
    /// in order to watch the first result of a query
    /// </summary>
    /// <typeparam name="T">type of object that is being observed</typeparam>
    public class QueryFirstResultStreamObserver<T> : IAsyncObserver<T>
    {
        private Action<T> _action;
        public QueryFirstResultStreamObserver(Action<T> action)
        {
            this._action = action;
        }

        public Task OnCompletedAsync()
        {
            //return TaskDone.Done;
            return Task.CompletedTask;
        }

        public Task OnErrorAsync(Exception ex)
        {
            throw ex;
        }

        public Task OnNextAsync(T item, StreamSequenceToken token = null)
        {
            if (this._action != null)
            {
                this._action(item);
                this._action = null;
            }
            //return TaskDone.Done;
            return Task.CompletedTask;
        }
    }
}
