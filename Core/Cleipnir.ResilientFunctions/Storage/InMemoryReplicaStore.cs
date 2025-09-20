using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Storage;

public class InMemoryReplicaStore : IReplicaStore
{
    private readonly Dictionary<ReplicaId, long> _replicas = new();
    private readonly Lock _sync = new();
    
    public Task Initialize() => Task.CompletedTask;

    public Task Insert(ReplicaId replicaId, long timestamp)
    {
        lock (_sync)
            _replicas.TryAdd(replicaId, timestamp);
        
        return Task.CompletedTask;
    }

    public Task Delete(ReplicaId replicaId)
    {
        lock (_sync)
            _replicas.Remove(replicaId);
        
        return Task.CompletedTask;
    }

    public Task<bool> UpdateHeartbeat(ReplicaId replicaId, long timestamp)
    {
        lock (_sync)
            if (_replicas.ContainsKey(replicaId))
            {
                _replicas[replicaId] = timestamp;
                return true.ToTask();
            }

        
        return false.ToTask();            
    }

    public Task<IReadOnlyList<StoredReplica>> GetAll()
    {
        lock (_sync)
            return _replicas
                .Select(kv => new StoredReplica(kv.Key, kv.Value))
                .ToList()
                .CastTo<IReadOnlyList<StoredReplica>>()
                .ToTask();
    }
}