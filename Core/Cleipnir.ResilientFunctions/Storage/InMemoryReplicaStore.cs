using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Storage;

public class InMemoryReplicaStore : IReplicaStore
{
    private readonly Dictionary<Guid, long> _replicas = new();

    public Task Initialize() => Task.CompletedTask;

    public Task Truncate()
    {
        lock (_replicas)
            _replicas.Clear();
        
        return Task.CompletedTask;
    }

    public Task Insert(Guid replicaId, long ttl) => Update(replicaId, ttl);

    public Task Update(Guid replicaId, long ttl)
    {
        lock (_replicas)
            _replicas[replicaId] = ttl;

        return Task.CompletedTask;
    }

    public Task Delete(Guid replicaId)
    {
        lock (_replicas)
            _replicas.Remove(replicaId);

        return Task.CompletedTask;
    }

    public Task Prune(long currentTime)
    {
        lock (_replicas)
            foreach (var id in _replicas.Keys.ToList())
                if (_replicas[id] < currentTime)
                    _replicas.Remove(id);

        return Task.CompletedTask;
    }

    public Task<int> GetReplicaCount()
    {
        lock (_replicas)
            return _replicas.Count.ToTask();
    }
}