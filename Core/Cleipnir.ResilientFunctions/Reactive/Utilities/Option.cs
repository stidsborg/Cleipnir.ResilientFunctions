using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;

namespace Cleipnir.ResilientFunctions.Reactive.Utilities;

public sealed class Option<T>(T v, bool hasValue) : ICustomSerializable
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
        => serializer.Serialize(HasValue ? [v] : new List<T>());
    public static object Deserialize(byte[] bytes, ISerializer serializer)
    {
        var list = serializer.Deserialize<List<T>>(bytes);
        return list.Count == 0 ? Option<T>.NoValue : new Option<T>(list[0], hasValue: true);
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