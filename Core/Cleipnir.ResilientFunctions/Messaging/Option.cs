using System;
using System.Threading.Tasks;

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

public static class Option
{
    public static async Task<T> Value<T>(this Task<Option<T>> option)
        => (await option).Value;
    
    public static async Task<bool> HasValue<T>(this Task<Option<T>> option)
        => (await option).HasValue;

    public static Option<T> Create<T>(T value) => new Option<T>(value);
    public static Option<T> CreateNoValue<T>() => new Option<T>();
}