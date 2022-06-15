using System.Text.Json;

namespace Cleipnir.ResilientFunctions.Messaging.Tests.Utils;

public static class JsonExtensions
{
    public static string ToJson(this object o) => JsonSerializer.Serialize(o, o.GetType());
    public static T? DeserializeInto<T>(this string json) => JsonSerializer.Deserialize<T>(json);
    public static object? DeserializeInto(this string json, Type type) => JsonSerializer.Deserialize(json, type);
}