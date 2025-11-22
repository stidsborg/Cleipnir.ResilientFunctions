using System;
using System.Linq;

namespace Cleipnir.ResilientFunctions.Domain;

public record EffectId(int Id, int[] Context)
{
    public virtual bool Equals(EffectId? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        return Id == other.Id && Context.SequenceEqual(other.Context);
    }

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Id);
        foreach (var c in Context)
            hash.Add(c);
        return hash.ToHashCode();
    }

    public SerializedEffectId Serialize()
    {
        if (Context.Length == 0)
            return new SerializedEffectId([Id]);

        return new SerializedEffectId([..Context, Id]);
    }

    public static EffectId Deserialize(SerializedEffectId serialized) => Deserialize(serialized.Value);
    public static EffectId Deserialize(int[] serialized)
    {
        if (serialized.Length == 0)
            throw new ArgumentException("Serialized EffectId cannot be empty", nameof(serialized));

        var id = serialized[^1];
        var context = serialized.Length > 1
            ? serialized[..^1]
            : Array.Empty<int>();

        return new EffectId(id, context);
    }

    public static EffectId CreateWithRootContext(int id)
        => new(id, Context: Array.Empty<int>());

    public static EffectId CreateWithCurrentContext(int id)
    {
        var parent = EffectContext.CurrentContext.Parent;
        if (parent == null)
            return new EffectId(id, Array.Empty<int>());

        var parentContext = parent.Context;
        var newContext = new int[parentContext.Length + 1];
        Array.Copy(parentContext, newContext, parentContext.Length);
        newContext[^1] = parent.Id;
        return new EffectId(id, newContext);
    }
}

public record SerializedEffectId(int[] Value)
{
    public virtual bool Equals(SerializedEffectId? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        return Value.SequenceEqual(other.Value);
    }

    public override int GetHashCode()
    {
        var hash = new HashCode();
        foreach (var v in Value)
            hash.Add(v);
        return hash.ToHashCode();
    }

    public string ToStringValue() => string.Join(".", Value);
}

public static class EffectIdExtensions
{
    public static EffectId ToEffectId(this int value, int[]? context = null) => new(value, context ?? Array.Empty<int>());
    public static SerializedEffectId ToSerializedEffectId(this int[] value) => new(value);
}