using System;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class ClusterInfo(ReplicaId replicaId)
{
    public ReplicaId ReplicaId { get; } = replicaId;

    private ulong _offset;
    public ulong Offset
    {
        get
        {
            lock (_sync)
                return _offset;
        }
        internal set
        {
            lock (_sync)
                _offset = value;
        }
    }

    private ulong _replicaCount;
    public ulong ReplicaCount
    {
        get
        {
            lock (_sync)
                return _replicaCount;
        }
        internal set
        {
            lock (_sync)
                _replicaCount = value;
        }
    }

    private readonly Lock _sync = new();

    public bool OwnedByThisReplica(StoredId storedId)
    {
        var serializedStoredId = storedId.Serialize();
        using SHA256 sha256 = SHA256.Create();
        var hashBytes = sha256.ComputeHash(Encoding.UTF8.GetBytes(serializedStoredId));
        var number = BitConverter.ToUInt64(hashBytes);
        var owner = number % ReplicaCount;
        return Offset == owner;
    }
}