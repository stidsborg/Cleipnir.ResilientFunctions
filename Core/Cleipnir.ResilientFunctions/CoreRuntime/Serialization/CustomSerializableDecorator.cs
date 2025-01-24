using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
/*
public class CustomSerializableDecorator : ISerializer
{
    private readonly ISerializer _inner;
    private readonly Dictionary<Type, Func<byte[], ISerializer, object>> _deserializers = new();
    private readonly Lock _lock = new();

    public CustomSerializableDecorator(ISerializer inner) => _inner = inner;
    
    public byte[] SerializeParameter<TParam>(TParam parameter) 
        => parameter is ICustomSerializable customSerializable
            ? customSerializable.Serialize(this)
            : _inner.SerializeParameter(parameter);
    
    public TParam DeserializeParameter<TParam>(byte[] json)
    {
        return typeof(TParam).IsAssignableTo(typeof(ICustomSerializable)) 
            ? CustomDeserialize<TParam>(json) 
            : _inner.DeserializeParameter<TParam>(json);
    }
    
    public StoredException SerializeException(FatalWorkflowException exception)
        => _inner.SerializeException(exception);

    public FatalWorkflowException DeserializeException(FlowId flowId, StoredException storedException) 
        => _inner.DeserializeException(flowId, storedException);

    public byte[] SerializeResult<TResult>(TResult result)
        => result is ICustomSerializable customSerializable
            ? customSerializable.Serialize(this)
            : _inner.SerializeResult(result);
    
    public TResult DeserializeResult<TResult>(byte[] json) 
    {
        return typeof(TResult).IsAssignableTo(typeof(ICustomSerializable)) 
            ? CustomDeserialize<TResult>(json) 
            : _inner.DeserializeParameter<TResult>(json);
    }

    public SerializedMessage SerializeMessage<TEvent>(TEvent message) where TEvent : notnull
        => _inner.SerializeMessage(message); //todo allow custom serializer
    public object DeserializeMessage(byte[] json, byte[] type) => _inner.DeserializeMessage(json, type);

    public byte[] SerializeEffectResult<TResult>(TResult result)
        => result is ICustomSerializable customSerializable
            ? customSerializable.Serialize(this)
            : _inner.SerializeEffectResult(result);
    
    public TResult DeserializeEffectResult<TResult>(byte[] json)
    {
        return typeof(TResult).IsAssignableTo(typeof(ICustomSerializable)) 
            ? CustomDeserialize<TResult>(json) 
            : _inner.DeserializeEffectResult<TResult>(json);
    }

    public byte[] SerializeState<TState>(TState state) where TState : FlowState, new()
        => _inner.SerializeState(state);
    public TState DeserializeState<TState>(byte[] json) where TState : FlowState, new()
        => _inner.DeserializeState<TState>(json);
    
    private T CustomDeserialize<T>(byte[] bytes)
    {
        lock (_lock)
        {
            if (!_deserializers.ContainsKey(typeof(T)))
            {
                //var serializeMethodInfo = typeof(T).GetMethod(nameof(ICustomSerializable.Serialize), BindingFlags.Public | BindingFlags.Static);
                //var serializeFunc = (Func<ISerializer, byte[]>) Delegate.CreateDelegate(typeof(Func<ISerializer, byte[]>), serializeMethodInfo!);
                var deserializeMethodInfo = typeof(T).GetMethod(nameof(ICustomSerializable.Deserialize), BindingFlags.Public | BindingFlags.Static);
                var deserializeFunc = (Func<byte[], ISerializer, object>) Delegate.CreateDelegate(typeof(Func<byte[], ISerializer, object>), deserializeMethodInfo!);
                _deserializers[typeof(T)] = deserializeFunc;
            }

            return (T) _deserializers[typeof(T)](bytes, this);
        }
    }
}*/