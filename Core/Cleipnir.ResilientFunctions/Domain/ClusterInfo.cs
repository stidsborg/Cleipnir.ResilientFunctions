using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
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
        => ResponsibleReplica(storedId) == ReplicaId;

    /// <summary>
    /// Maps the provided id to the replica responsible for it using rendezvous (highest-random-weight) hashing -
    /// each replica is scored by hashing the id together with the replica's id and the highest score wins. Thus,
    /// a membership change only remaps the ids scored highest by the joining/leaving replica (~1/n of all ids).
    /// </summary>
    public ReplicaId ResponsibleReplica(StoredId storedId)
    {
        var replicas = Replicas;
        if (replicas.Count == 0)
            throw new InvalidOperationException("Cannot map to responsible replica - cluster membership has not been initialized");

        // Ties are broken by strict comparison - the earliest replica in the ascendingly ordered list wins,
        // so all replicas agree on the responsible replica.
        var responsible = replicas[0];
        var maxScore = Score(storedId, responsible);
        for (var i = 1; i < replicas.Count; i++)
        {
            var score = Score(storedId, replicas[i]);
            if (score > maxScore)
            {
                maxScore = score;
                responsible = replicas[i];
            }
        }

        return responsible;
    }

    private static ulong Score(StoredId storedId, ReplicaId replicaId)
    {
        Span<byte> buffer = stackalloc byte[32];
        storedId.AsGuid.TryWriteBytes(buffer);
        replicaId.AsGuid.TryWriteBytes(buffer[16..]);
        Span<byte> hash = stackalloc byte[SHA256.HashSizeInBytes];
        SHA256.HashData(buffer, hash);
        return BitConverter.ToUInt64(hash);
    }

    public override string ToString() => $"{ReplicaId.AsGuid} ({Offset}/{ReplicaCount} count)";
}