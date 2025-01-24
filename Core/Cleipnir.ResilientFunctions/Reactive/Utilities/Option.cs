using System;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;

namespace Cleipnir.ResilientFunctions.Reactive.Utilities;

public sealed class Option<T>(T v, bool hasValue)
{
    public static Option<T> NoValue { get; } = new(default!, hasValue: false);
    public bool HasValue { get; } = hasValue;

    [JsonInclude]
    private T V => v;
    
    [JsonIgnore]
    public T Value => HasValue ? v! : throw new InvalidOperationException("Option does not have a value");

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

    public byte[] Serialize(ISerializer serializer)
    {
        if (!HasValue)
            return [];
        
        var valueBytes = serializer.Serialize(Value);
        var bytes = new byte[valueBytes.Length + 1];
        bytes[0] = 1;
        valueBytes.CopyTo(bytes, index: 1);
        return bytes;
    }

    public static object Deserialize(byte[] bytes, ISerializer serializer)
    {
        if (bytes[0] == 0)
            return NoValue;

        var value = (T) serializer.Deserialize<T>(bytes[1..])!;
        return new Option<T>(value, hasValue: true);
    }
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