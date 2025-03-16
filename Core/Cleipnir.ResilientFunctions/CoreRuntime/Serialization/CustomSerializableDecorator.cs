using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Serialization;

public class CustomSerializableDecorator : ISerializer
{
    private readonly ISerializer _inner;
    private readonly Dictionary<Type, Func<byte[], ISerializer, object>> _deserializers = new();
    private readonly Lock _lock = new();

    public CustomSerializableDecorator(ISerializer inner) => _inner = inner;
    
    public byte[] Serialize<T>(T parameter) 
        => parameter is ICustomSerializable customSerializable
            ? customSerializable.Serialize(this)
            : _inner.Serialize(parameter);

    public byte[] Serialize(object? value, Type type) => _inner.Serialize(value, type);

    public T Deserialize<T>(byte[] json) 
        => typeof(T).IsAssignableTo(typeof(ICustomSerializable)) 
            ? CustomDeserialize<T>(json) 
            : _inner.Deserialize<T>(json);
    
    public StoredException SerializeException(FatalWorkflowException exception)
        => _inner.SerializeException(exception);

    public FatalWorkflowException DeserializeException(FlowId flowId, StoredException storedException) 
        => _inner.DeserializeException(flowId, storedException);

    public SerializedMessage SerializeMessage(object message, Type messageType)
        => _inner.SerializeMessage(message, messageType);
    
    public object DeserializeMessage(byte[] json, byte[] type) => _inner.DeserializeMessage(json, type);
    
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