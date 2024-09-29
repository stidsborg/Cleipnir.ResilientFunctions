using System;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IReplicaStore
{
    Task Initialize();
    Task Truncate();

    Task Insert(Guid replicaId, long ttl);
    Task Update(Guid replicaId, long ttl);
    Task Delete(Guid replicaId);
    Task Prune(long currentTime);
    Task<int> GetReplicaCount();
}