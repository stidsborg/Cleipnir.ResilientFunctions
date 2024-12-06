using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Storage;

public class InMemoryCemaphoreStore : ICemaphoreStore
{
    private readonly Dictionary<Tuple<string, string>, List<StoredId>> _semaphores = new();
    private readonly Lock _lock = new();
    
    public Task<bool> Acquire(string group, string instance, StoredId storedId, int semaphoreCount)
    {
        lock (_lock)
        {
            var key = Tuple.Create(group, instance);
            if (!_semaphores.ContainsKey(key))
                _semaphores[key] = new();

            var queue = _semaphores[key];
            for (var i = 0; i < queue.Count; i++)
                if (queue[i] == storedId)
                    return (i < semaphoreCount).ToTask();
            
            queue.Add(storedId);
            return (queue.Count <= semaphoreCount).ToTask();
        }
    }

    public Task<IReadOnlyList<StoredId>> Release(string group, string instance, StoredId storedId, int semaphoreCount)
    {
        lock (_lock)
        {
            var key = Tuple.Create(group, instance);
            if (!_semaphores.ContainsKey(key))
                _semaphores[key] = new();
            
            _semaphores[key] = _semaphores[key].Where(id => id != storedId).ToList();

            return _semaphores[key].Take(semaphoreCount).ToList().CastTo<IReadOnlyList<StoredId>>().ToTask();
        }
    }

    public Task<IReadOnlyList<StoredId>> GetQueued(string group, string instance, int count)
    {
        lock (_lock)
            return _semaphores[Tuple.Create(group, instance)]
                .Take(count)
                .ToList()
                .CastTo<IReadOnlyList<StoredId>>()
                .ToTask();
    }
}