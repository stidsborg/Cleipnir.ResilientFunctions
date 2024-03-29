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
    
    public StoredParameter SerializeParameter<TParam>(TParam parameter) where TParam : notnull
        => new(JsonSerializer.Serialize(parameter, parameter.GetType()), parameter.GetType().SimpleQualifiedName());
    public TParam DeserializeParameter<TParam>(string json, string type) where TParam : notnull
        => (TParam) JsonSerializer.Deserialize(json, Type.GetType(type, throwOnError: true)!)!;
    
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

    public StoredResult SerializeResult<TResult>(TResult result)
        => new(JsonSerializer.Serialize(result, result?.GetType() ?? typeof(TResult)), result?.GetType().SimpleQualifiedName());
    public TResult DeserializeResult<TResult>(string json, string type) 
        => (TResult) JsonSerializer.Deserialize(json, Type.GetType(type, throwOnError: true)!)!;

    public JsonAndType SerializeMessage<TEvent>(TEvent message) where TEvent : notnull 
        => new(JsonSerializer.Serialize(message, message.GetType()), message.GetType().SimpleQualifiedName());
    public object DeserializeMessage(string json, string type)
        => JsonSerializer.Deserialize(json, Type.GetType(type, throwOnError: true)!)!;

    public string SerializeEffectResult<TResult>(TResult result)
        => JsonSerializer.Serialize(result);
    public TResult DeserializeEffectResult<TResult>(string json)
        => JsonSerializer.Deserialize<TResult>(json)!;

    public JsonAndType SerializeState<TState>(TState state) where TState : WorkflowState, new()
        => new(JsonSerializer.Serialize(state), typeof(TState).SimpleQualifiedName());
    public WorkflowState DeserializeState(string json, string type)
        => (WorkflowState) JsonSerializer.Deserialize(json, Type.GetType(type, throwOnError: true)!)!;
}