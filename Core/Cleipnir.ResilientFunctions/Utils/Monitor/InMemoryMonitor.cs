using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Utils.Monitor;

public class InMemoryMonitor : IMonitor
{
    private readonly Dictionary<string, string> _locks = new();
    private readonly object _sync = new();

    public int LockCount
    {
        get
        {
            lock (_sync)
                return _locks.Count;
        }
    }

    public Task<IMonitor.ILock?> Acquire(string lockId, string keyId)
    {
        lock (_sync)
        {
            if (_locks.ContainsKey(lockId) && _locks[lockId] != keyId) 
                return default(IMonitor.ILock).ToTask(); //lock is taken by someone else
            
            _locks[lockId] = keyId;
            return new Lock(this, lockId, keyId).CastTo<IMonitor.ILock?>().ToTask();
        }
    }
    
    public Task Release(string lockId, string keyId)
    {
        lock (_sync)
        {
            if (_locks.ContainsKey(lockId) && _locks[lockId] == keyId)
                _locks.Remove(lockId);

            return Task.CompletedTask;
        }
    }

    private class Lock : IMonitor.ILock
    {
        private readonly InMemoryMonitor _monitor;
        private readonly string _lockId;
        private readonly string _keyId;

        public Lock(InMemoryMonitor monitor, string lockId, string keyId)
        {
            _monitor = monitor;
            _lockId = lockId;
            _keyId = keyId;
        }

        public ValueTask DisposeAsync()
        {
            var task = _monitor.Release(_lockId, _keyId);
            return new ValueTask(task);
        }
    }
}