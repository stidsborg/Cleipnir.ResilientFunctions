using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Watchdogs;

public class AsyncSemaphore
{
    private int _freeWorkers;
    private readonly Queue<TcsAndLock> _queue = new();
    private readonly object _sync = new();

    public AsyncSemaphore(int maxParallelism) => _freeWorkers = maxParallelism;

    public Task<IDisposable> Take()
    {
        var tcs = new TaskCompletionSource<IDisposable>();
        var @lock = new Lock();
        var tcsAndLock = new TcsAndLock(tcs, @lock);
        
        lock (_sync)
        {
            if (_freeWorkers == 0)
            {
                _queue.Enqueue(tcsAndLock);
                return tcs.Task;
            }

            _freeWorkers--;
        }

        _ = ExecuteWorker(tcsAndLock);
        return tcs.Task;
    }

    private async Task ExecuteWorker(TcsAndLock? tcsAndLock)
    {
        if (tcsAndLock != null)
        {
            var (tcs, @lock) = tcsAndLock;
            
            tcs.TrySetResult(@lock);
            await @lock.Disposed.Task;
        }
        
        while (true)
        {
            TaskCompletionSource<IDisposable> tcs;
            Lock @lock;
            
            lock (_sync)
            {
                if (_queue.Count == 0)
                {
                    _freeWorkers++;
                    return;
                }
                
                tcsAndLock = _queue.Dequeue();
                tcs = tcsAndLock.Tcs;
                @lock = tcsAndLock.Lock;
            }
            
            tcs.TrySetResult(@lock);
            await @lock.Disposed.Task;
        }
    }

    private class Lock : IDisposable
    {
        public TaskCompletionSource Disposed { get;  } = new();

        public void Dispose() => Disposed.TrySetResult();
    }

    private record TcsAndLock(TaskCompletionSource<IDisposable> Tcs, Lock Lock);
}