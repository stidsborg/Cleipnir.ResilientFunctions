using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IReplicaStore
{
    public Task Initialize();
    public Task Insert(ReplicaId replicaId, long timeStamp);
    public Task Delete(ReplicaId replicaId);
    public Task<bool> UpdateHeartbeat(ReplicaId replicaId, long timeStamp);
    public Task<IReadOnlyList<StoredReplica>> GetAll(long olderThan);
}