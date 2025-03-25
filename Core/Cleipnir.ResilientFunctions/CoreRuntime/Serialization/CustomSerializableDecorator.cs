using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Serialization;

public class CustomSerializableDecorator(ISerializer inner) : ISerializer
{
    private readonly Dictionary<Type, Func<byte[], ISerializer, object>> _deserializers = new();
    private readonly Lock _lock = new();

    public byte[] Serialize<T>(T parameter) 
        => parameter is ICustomSerializable customSerializable
            ? customSerializable.Serialize(this)
            : inner.Serialize(parameter);

    public byte[] Serialize(object? value, Type type) => 
        value is ICustomSerializable customSerializable
            ? customSerializable.Serialize(this)
            : inner.Serialize(value, type);

    public T Deserialize<T>(byte[] json) 
        => typeof(T).IsAssignableTo(typeof(ICustomSerializable)) 
            ? CustomDeserialize<T>(json) 
            : inner.Deserialize<T>(json);
    
    public StoredException SerializeException(FatalWorkflowException exception)
        => inner.SerializeException(exception);

    public FatalWorkflowException DeserializeException(FlowId flowId, StoredException storedException) 
        => inner.DeserializeException(flowId, storedException);

    public SerializedMessage SerializeMessage(object message, Type messageType)
        => inner.SerializeMessage(message, messageType);
    
    public object DeserializeMessage(byte[] json, byte[] type) => inner.DeserializeMessage(json, type);
    
    private T CustomDeserialize<T>(byte[] bytes)
    {
        lock (_lock)
        {
            if (!_deserializers.ContainsKey(typeof(T)))
            {
                var deserializeMethodInfo = typeof(T).GetMethod(nameof(ICustomSerializable.Deserialize), BindingFlags.Public | BindingFlags.Static);
                var deserializeFunc = (Func<byte[], ISerializer, object>) Delegate.CreateDelegate(typeof(Func<byte[], ISerializer, object>), deserializeMethodInfo!);
                _deserializers[typeof(T)] = deserializeFunc;
            }

            return (T) _deserializers[typeof(T)](bytes, this);
        }
    }
}

public static class CustomSerializableDecoratorExtensions
{
    public static ISerializer DecorateWithCustomerSerializableHandling(this ISerializer serializer)
        => new CustomSerializableDecorator(serializer);
} 