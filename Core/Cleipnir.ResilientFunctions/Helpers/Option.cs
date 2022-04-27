namespace Cleipnir.ResilientFunctions.Helpers;

public struct Option<T>
{
    public T? Value { get; }
    public bool HasValue { get; }

    public Option()
    {
        Value = default;
        HasValue = false;
    }

    public Option(T value)
    {
        Value = value;
        HasValue = true;
    }
}