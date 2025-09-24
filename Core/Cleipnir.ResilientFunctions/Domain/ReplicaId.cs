using System;

namespace Cleipnir.ResilientFunctions.Domain;

public record ReplicaId(Guid AsGuid) : IComparable<ReplicaId>
{
    public static ReplicaId NewId() => new(Guid.NewGuid());
    public static ReplicaId Empty => new(Guid.Empty);
    
    public static ReplicaId Parse(string s) => new(Guid.Parse(s));
    
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
    public static ReplicaId ParseToReplicaId(this string replicaId) => new(Guid.Parse(replicaId));
}