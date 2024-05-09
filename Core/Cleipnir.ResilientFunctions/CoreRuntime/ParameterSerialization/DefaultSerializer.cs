using System;
using System.Text.Json;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;

public class DefaultSerializer : ISerializer
{
    public static readonly DefaultSerializer Instance = new();
    private DefaultSerializer() {}
    
    public string SerializeParameter<TParam>(TParam parameter)  
        => JsonSerializer.Serialize(parameter);
    public TParam DeserializeParameter<TParam>(string json) 
        => JsonSerializer.Deserialize<TParam>(json)!;
    
    public StoredException SerializeException(Exception exception)
        => new StoredException(
            exception.Message,
            exception.StackTrace,
            ExceptionType: exception.GetType().SimpleQualifiedName()
        );

    public PreviouslyThrownException DeserializeException(StoredException storedException)
        => new PreviouslyThrownException(
            storedException.ExceptionMessage,
            storedException.ExceptionStackTrace,
            Type.GetType(storedException.ExceptionType, throwOnError: true)!
        );

    public string SerializeResult<TResult>(TResult result)  
        => JsonSerializer.Serialize(result);
    public TResult DeserializeResult<TResult>(string json)  
        => JsonSerializer.Deserialize<TResult>(json)!;

    public JsonAndType SerializeMessage<TEvent>(TEvent message) where TEvent : notnull 
        => new(JsonSerializer.Serialize(message, message.GetType()), message.GetType().SimpleQualifiedName());
    public object DeserializeMessage(string json, string type)
        => JsonSerializer.Deserialize(json, Type.GetType(type, throwOnError: true)!)!;

    public string SerializeEffectResult<TResult>(TResult result)
        => JsonSerializer.Serialize(result);
    public TResult DeserializeEffectResult<TResult>(string json)
        => JsonSerializer.Deserialize<TResult>(json)!;

    public string SerializeState<TState>(TState state) where TState : WorkflowState, new()
        => JsonSerializer.Serialize(state);
    public TState DeserializeState<TState>(string json) where TState : WorkflowState, new()
        => JsonSerializer.Deserialize<TState>(json)!;
}