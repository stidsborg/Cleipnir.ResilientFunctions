﻿using System;
using System.Runtime.Serialization;
using Newtonsoft.Json;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

public static class Json
{
    private static JsonSerializerSettings SerializerSettings { get; } =
        new JsonSerializerSettings()
        {
            TypeNameHandling = TypeNameHandling.Auto
        };

    public static string ToJson<T>(this T t) => JsonConvert.SerializeObject(t, typeof(T), SerializerSettings);
    public static byte[] ToJsonByteArray<T>(this T t) => System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(t);

    public static object DeserializeFromJsonTo(this string json, Type type) => Deserialize(json, type);
    public static T DeserializeFromJsonTo<T>(this string json) => (T) Deserialize(json, typeof(T));
    
    public static object Deserialize(string json, Type type)
    {
        var deserialized = JsonConvert.DeserializeObject(json, type, SerializerSettings);
        if (deserialized == null)
            throw new SerializationException("Json deserialized to null");

        return deserialized;
    }
}