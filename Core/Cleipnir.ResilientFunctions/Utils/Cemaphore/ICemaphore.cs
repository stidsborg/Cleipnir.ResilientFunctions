using System;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Utils.Cemaphore;

public interface ICemaphore
{
    public Task<ILock> Acquire(string group, string name, TimeSpan? maxWait = null);
    public Task<ILock?> TryAcquire(TimeSpan? maxWait = null);

    public Task Release(string group, string name, string lockId);

    public interface ILock
    {
        public Task DoThenRelease(Action action);
        public Task DoThenRelease(Func<Task> func);
        public Task<T> DoThenRelease<T>(Func<Task<T>> func);
        public Task Release();
    }
}