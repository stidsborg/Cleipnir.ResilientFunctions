using System;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Messaging;

public class Option<T>(T value, bool hasValue)
{
    public static Option<T> NoValue { get; } = new(default!, hasValue: false);
    private readonly T? _t = value;
    public bool HasValue { get; } = hasValue;
    public T Value => HasValue ? _t! : throw new InvalidOperationException("Object must have a value");

    public override bool Equals(object? obj)
    {
        if (obj is not Option<T> option)
            return false;

        if (!HasValue == !option.HasValue)
            return true;
        if (!HasValue || !option.HasValue)
            return false;

        return Value!.Equals(option.Value);
    }
    public override int GetHashCode() => !HasValue ? 0 : Value!.GetHashCode();
}

public static class Option
{
    public static async Task<T> Value<T>(this Task<Option<T>> option)
        => (await option).Value;
    
    public static async Task<bool> HasValue<T>(this Task<Option<T>> option)
        => (await option).HasValue;

    public static Option<T> Create<T>(T value) => new(value, hasValue: true);
    public static Option<T> CreateNoValue<T>() => new(default!, hasValue: false);
}