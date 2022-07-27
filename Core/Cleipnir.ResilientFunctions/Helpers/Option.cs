namespace Cleipnir.ResilientFunctions.Helpers;

public static class Option
{
    public static Option<T> Some<T>(T value) => new(value);
}

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

    public static Option<T> None { get; } = new();
}