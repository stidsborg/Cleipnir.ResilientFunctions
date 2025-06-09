using System;

namespace Cleipnir.ResilientFunctions.Domain;

public record ReplicaId(Guid AsGuid) : IComparable<ReplicaId>
{
    public static ReplicaId NewId() => new ReplicaId(Guid.NewGuid());
    
    public int CompareTo(ReplicaId? other)
    {
        if (ReferenceEquals(this, other)) return 0;
        if (other is null) return 1;
        return AsGuid.CompareTo(other.AsGuid);
    }
}

public static class ReplicaIdExtensions
{
    public static ReplicaId ToReplicaId(this Guid replicaId) => new(replicaId);
}