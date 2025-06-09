using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IReplicaStore
{
    public Task Initialize();
    public Task Insert(ReplicaId replicaId);
    public Task Delete(ReplicaId replicaId);
    public Task UpdateHeartbeat(ReplicaId replicaId);
    public Task<IReadOnlyList<StoredReplica>> GetAll();
}