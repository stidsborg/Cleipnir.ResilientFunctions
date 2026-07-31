using System;
using System.Collections.Generic;
using System.Linq;
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

    // The leader is the replica with the lowest id - i.e. the replica with offset 0 in the ascendingly ordered replica ids
    public bool IsLeader
    {
        get
        {
            lock (_sync)
                return _replicaCount > 0 && _offset == 0;
        }
    }

    private IReadOnlyList<ReplicaId> _replicas = [];
    /// <summary>
    /// All replicas currently in the cluster, ascendingly ordered - a replica's <see cref="Offset"/> is its
    /// index in this list. Maintained by the ReplicaWatchdog alongside <see cref="Offset"/> and
    /// <see cref="ReplicaCount"/>.
    /// </summary>
    public IReadOnlyList<ReplicaId> Replicas
    {
        get
        {
            lock (_sync)
                return _replicas;
        }
        internal set
        {
            var ordered = value.Order().ToList();
            lock (_sync)
                _replicas = ordered;
        }
    }

    private readonly Lock _sync = new();

    public bool OwnedByThisReplica(StoredId storedId)
    {
        var owner = Hash(storedId) % ReplicaCount;
        return Offset == owner;
    }

    /// <summary>
    /// Maps the provided id to the replica responsible for it - the same hash-modulo-replica-count sharding over
    /// the ascendingly ordered replica ids that <see cref="OwnedByThisReplica"/> uses.
    /// </summary>
    public ReplicaId ResponsibleReplica(StoredId storedId)
    {
        var replicas = Replicas;
        if (replicas.Count == 0)
            throw new InvalidOperationException("Cannot map to responsible replica - cluster membership has not been initialized");

        return replicas[(int)(Hash(storedId) % (ulong)replicas.Count)];
    }

    private static ulong Hash(StoredId storedId)
    {
        var serializedStoredId = storedId.Serialize();
        using var sha256 = SHA256.Create();
        var hashBytes = sha256.ComputeHash(Encoding.UTF8.GetBytes(serializedStoredId));
        return BitConverter.ToUInt64(hashBytes);
    }

    public override string ToString() => $"{ReplicaId.AsGuid} ({Offset}/{ReplicaCount} count)";
}