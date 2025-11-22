namespace Cleipnir.ResilientFunctions.Domain;

public class TimeoutId
{
    public int Value { get; }
    public TimeoutId(int value)
    {
        Value = value;
    }

    public static implicit operator TimeoutId(int timeoutId) => new(timeoutId);
    public override string ToString() => Value.ToString();
    public static bool operator ==(TimeoutId id1, TimeoutId id2) => id1.Equals(id2);
    public static bool operator !=(TimeoutId id1, TimeoutId id2) => !(id1 == id2);

    public override bool Equals(object? obj)
        => obj is TimeoutId id && id.Value == Value;
    public override int GetHashCode() => Value.GetHashCode();
}