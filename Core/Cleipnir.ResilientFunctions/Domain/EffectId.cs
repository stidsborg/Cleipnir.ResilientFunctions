using System;
using System.Linq;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Domain;

public record EffectId(int[] Value)
{
    public int Id => Value[^1];
    public int[] Context => Value.Length > 1 ? Value[..^1] : [];

    public virtual bool Equals(EffectId? other)
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

    public SerializedEffectId Serialize() => new(Value);

    public static EffectId Deserialize(SerializedEffectId serialized) => new(serialized.Value);
    public static EffectId Deserialize(int[] serialized)
    {
        if (serialized.Length == 0)
            throw new ArgumentException("Serialized EffectId cannot be empty", nameof(serialized));

        return new EffectId(serialized);
    }

    public static EffectId CreateWithRootContext(int id) => new([id]);

    public static EffectId CreateWithCurrentContext(int id)
    {
        var parent = EffectContext.CurrentContext.Parent;
        if (parent == null)
            return new EffectId([id]);

        return new EffectId([..parent.Value, id]);
    }

    public EffectId CreateChild(int id) => new(Value.Append(id).ToArray());

    public bool IsDescendant(EffectId descendant)
    {
        if (descendant.Value.Length >= Value.Length)
            return false;
        
        for (var i = 0; i < Value.Length && i < descendant.Value.Length; i++)
            if (Value[i] != descendant.Value[i])
                return false;

        return true;
    }
    
    public bool IsChild(EffectId child)
    {
        if (child.Value.Length != Value.Length + 1)
            return false;
        
        for (var i = 0; i < Value.Length; i++)
            if (Value[i] != child.Value[i])
                return false;

        return true;
    }

    public override string ToString() => "[" + Value.Select(v => v.ToString()).StringJoin(",") + "]";
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
    public static EffectId ToEffectId(this int value) => new([value]);
    public static SerializedEffectId ToSerializedEffectId(this int[] value) => new(value);
}
