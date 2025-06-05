using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IReplicaStore
{
    public Task Initialize();
    public Task Insert(Guid replicaId);
    public Task Delete(Guid replicaId);
    public Task UpdateHeartbeat(Guid replicaId);
    public Task<IReadOnlyList<StoredReplica>> GetAll();
}