using System;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers.Disposables;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public class AsyncSemaphore
{
    private readonly SemaphoreSlim _semaphore;

    public AsyncSemaphore(int maxParallelism) => _semaphore = new SemaphoreSlim(maxParallelism);

    public async Task<IDisposable> Take()
    {
        await _semaphore.WaitAsync();
        return new Lock(_semaphore);
    }

    public bool TryTake(out IDisposable @lock)
    {
        var success = _semaphore.WaitAsync(timeout: TimeSpan.Zero).Result;
        @lock = success
            ? new Lock(_semaphore)
            : Disposable.NoOp();
        
        return success;
    }

    private class Lock : IDisposable
    {
        private readonly SemaphoreSlim _semaphore;
        private readonly System.Threading.Lock _sync = new();
        private bool _disposed;

        public Lock(SemaphoreSlim semaphore) => _semaphore = semaphore;
        
        public void Dispose()
        {
            lock (_sync)
            {
                if (_disposed) return;
                _disposed = true;
                _semaphore.Release();
            }
        }
    }
}