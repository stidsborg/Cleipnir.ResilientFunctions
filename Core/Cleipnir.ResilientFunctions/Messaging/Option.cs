using System;

namespace Cleipnir.ResilientFunctions.Messaging;

public class Option<T>
{
    public static Option<T> NoValue { get; } = new();
    private readonly T? _t;
    public bool HasValue { get; }
    public T Value => HasValue ? _t! : throw new InvalidOperationException("Object must have a value");
    
    public Option() {}

    public Option(T value)
    {
        HasValue = true;
        _t = value;
    }
}