using System;
using System.Runtime.Serialization;
using Newtonsoft.Json;

namespace Cleipnir.ResilientFunctions.ParameterSerialization;

internal static class Json
{
    private static JsonSerializerSettings SerializerSettings { get; } = new() { TypeNameHandling = TypeNameHandling.Auto };

    public static string ToJson<T>(this T t) => JsonConvert.SerializeObject(t, typeof(T), SerializerSettings);
    public static object Deserialize(string json, Type type)
    {
        var deserialized = JsonConvert.DeserializeObject(json, type, SerializerSettings);
        if (deserialized == null)
            throw new SerializationException("Json deserialized to null");

        return deserialized;
    }
}