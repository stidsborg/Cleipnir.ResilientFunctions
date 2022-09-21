using System;
using System.Threading;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

public class AsyncSemaphore
{
    private readonly SemaphoreSlim _semaphore;

    public AsyncSemaphore(int maxParallelism) => _semaphore = new SemaphoreSlim(maxParallelism);

    public async Task<IDisposable> Take()
    {
        await _semaphore.WaitAsync();
        return new Lock(_semaphore);
    }

    private class Lock : IDisposable
    {
        private readonly SemaphoreSlim _semaphore;
        private readonly object _sync = new();
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