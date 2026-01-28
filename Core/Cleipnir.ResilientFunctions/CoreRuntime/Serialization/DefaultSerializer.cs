using System;
using System.Text.Json;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Serialization;

public class DefaultSerializer : ISerializer
{
    public static readonly ISerializer Instance = new DefaultSerializer();
    private DefaultSerializer() {}

    public byte[] Serialize(object value, Type type)
        => JsonSerializer.SerializeToUtf8Bytes(value, type);

    public object Deserialize(byte[] bytes, Type type)
        => JsonSerializer.Deserialize(bytes, type)!;
}