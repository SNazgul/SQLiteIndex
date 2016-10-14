using SQLiteIndex.Index;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace UISQLiteIndex.Index
{
    class SimpleDatabaseQueue : IDatabaseQueue
    {
        SQLiteConnection _conn;
        ConcurrentQueue<Action> _queue;
        Thread _watchThread;
        AutoResetEvent _threadMustBeStoped;
        AutoResetEvent _queueChanged;
        AutoResetEvent _actionExecuted;
        volatile int _isDisposed = 0;
        volatile int _currentSyncThread = -1;

        public SimpleDatabaseQueue(string pathToFileName)
        {
            SQLiteConnectionStringBuilder conString = new SQLiteConnectionStringBuilder()
            {
                DataSource = pathToFileName,
                FailIfMissing = true,
                ReadOnly = false
            };

            _conn = new SQLiteConnection(conString.ToString());
            _queue = new ConcurrentQueue<Action>();
            _threadMustBeStoped = new AutoResetEvent(false);
            _queueChanged = new AutoResetEvent(false);
            _actionExecuted = new AutoResetEvent(false);
            _watchThread = new Thread(this.WatchThread);
            _watchThread.Start();
        }

        public Task ExecuteAsync(Action<SQLiteConnection> act)
        {
            AutoResetEvent ev = new AutoResetEvent(false);
            _queue.Enqueue(() => ev.Set());
            _queueChanged.Set();

            Task task = Task.Factory.StartNew(
                () =>
                {
                    ev.WaitOne();

                    try
                    {
                        act(_isDisposed == 0 ? _conn : null);
                    }
                    finally
                    {
                        _actionExecuted.Set();
                    }
                }
                , CancellationToken.None, TaskCreationOptions.None,  TaskScheduler.Default);

            return task;
        }

        public void ExecuteSync(Action<SQLiteConnection> act)
        {
            if (_currentSyncThread == Thread.CurrentThread.ManagedThreadId)
            {
                act(_isDisposed == 0 ? _conn : null);
            }
            else
            {
                AutoResetEvent ev = new AutoResetEvent(false);
                _queue.Enqueue(() => ev.Set());
                _queueChanged.Set();
                ev.WaitOne();

                try
                {
                    act(_isDisposed == 0 ? _conn : null);
                }
                finally
                {
                    _actionExecuted.Set();
                }
            }
        }

        #region IDisposable Support

        protected virtual void Dispose(bool disposing)
        {
            int res = Interlocked.CompareExchange(ref _isDisposed, 1, 0);
            if (res == 0)
            {
                if (disposing)
                {
                    _threadMustBeStoped.Set();
                    _watchThread.Join();
                    _watchThread = null;
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~SimpleDatabaseQueue() {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
        #endregion

        void WatchThread()
        {
            var handles = new WaitHandle[2] { _threadMustBeStoped , _queueChanged };
            while(true)
            {
                int res = WaitHandle.WaitAny(handles);                
                while(!_queue.IsEmpty)
                {
                    Action act = null;
                    if (_queue.TryDequeue(out act))
                    {
                        try
                        {
                            _actionExecuted.Reset();
                            act();
                            _actionExecuted.WaitOne();
                        }
                        catch
                        {
                        }
                    }                   
                }

                if (res == 0)
                    break;
            }
        }
    }
}
