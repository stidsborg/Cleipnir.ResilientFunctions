﻿using System;
using System.Text;
using System.Text.Json;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Serialization;

public class DefaultSerializer : ISerializer
{
    public static readonly DefaultSerializer Instance = new();
    private DefaultSerializer() {}
    
    public byte[] SerializeParameter<TParam>(TParam parameter)  
        => JsonSerializer.SerializeToUtf8Bytes(parameter);
    public TParam DeserializeParameter<TParam>(byte[] json) 
        => JsonSerializer.Deserialize<TParam>(json)!;
    
    public StoredException SerializeException(FatalWorkflowException exception)
        => new(
            exception.FlowErrorMessage,
            exception.FlowStackTrace,
            ExceptionType: exception.ErrorType.SimpleQualifiedName()
        );

    public FatalWorkflowException DeserializeException(FlowId flowId, StoredException storedException)
        => FatalWorkflowException.Create(flowId, storedException);

    public byte[] SerializeResult<TResult>(TResult result)
        => JsonSerializer.SerializeToUtf8Bytes(result);
    public TResult DeserializeResult<TResult>(byte[] json)  
        => JsonSerializer.Deserialize<TResult>(json)!;

    public SerializedMessage SerializeMessage<TEvent>(TEvent message) where TEvent : notnull 
        => new(JsonSerializer.SerializeToUtf8Bytes(message, message.GetType()), message.GetType().SimpleQualifiedName().ToUtf8Bytes());

    public object DeserializeMessage(byte[] json, byte[] type)
        => JsonSerializer.Deserialize(json, Type.GetType(Encoding.UTF8.GetString(type), throwOnError: true)!)!;

    public byte[] SerializeEffectResult<TResult>(TResult result)
        => JsonSerializer.SerializeToUtf8Bytes(result);
    public TResult DeserializeEffectResult<TResult>(byte[] json)
        => JsonSerializer.Deserialize<TResult>(json)!;

    public byte[] SerializeState<TState>(TState state) where TState : FlowState, new()
        => JsonSerializer.SerializeToUtf8Bytes(state);
    public TState DeserializeState<TState>(byte[] json) where TState : FlowState, new()
        => JsonSerializer.Deserialize<TState>(json)!;
}