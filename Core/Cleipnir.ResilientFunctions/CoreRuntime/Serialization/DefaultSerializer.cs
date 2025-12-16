using System;
using System.Text;
using System.Text.Json;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Serialization;

public class DefaultSerializer : ISerializer
{
    public static readonly ISerializer Instance = new DefaultSerializer();
    private DefaultSerializer() {}

    public byte[] Serialize(object? value, Type type) => JsonSerializer.SerializeToUtf8Bytes(value, type);

    public void Serialize(object value, out byte[] valueBytes, out byte[] typeBytes)
    {
        valueBytes = JsonSerializer.SerializeToUtf8Bytes(value, value.GetType());
        typeBytes = value.GetType().SimpleQualifiedName().ToUtf8Bytes();
    }

    public object Deserialize(byte[] bytes, Type type) => JsonSerializer.Deserialize(bytes, type)!;

    public StoredException SerializeException(FatalWorkflowException exception)
        => new(
            exception.FlowErrorMessage,
            exception.FlowStackTrace,
            ExceptionType: exception.ErrorType.SimpleQualifiedName()
        );

    public FatalWorkflowException DeserializeException(FlowId flowId, StoredException storedException)
        => FatalWorkflowException.Create(flowId, storedException);

    public SerializedMessage SerializeMessage(object message, Type messageType)
        => new(JsonSerializer.SerializeToUtf8Bytes(message, message.GetType()), message.GetType().SimpleQualifiedName().ToUtf8Bytes());
    public object DeserializeMessage(byte[] json, byte[] type)
        => JsonSerializer.Deserialize(json, Type.GetType(Encoding.UTF8.GetString(type), throwOnError: true)!)!;
}