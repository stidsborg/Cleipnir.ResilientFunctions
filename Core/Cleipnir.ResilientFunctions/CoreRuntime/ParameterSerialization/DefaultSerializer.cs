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
        => new(JsonSerializer.Serialize(parameter), parameter.GetType().SimpleQualifiedName());
    public TParam DeserializeParameter<TParam>(string json, string type) where TParam : notnull
        => (TParam) JsonSerializer.Deserialize(json, Type.GetType(type, throwOnError: true)!)!;

    public StoredScrapbook SerializeScrapbook<TScrapbook>(TScrapbook scrapbook) where TScrapbook : RScrapbook
        => new(JsonSerializer.Serialize(scrapbook), scrapbook.GetType().SimpleQualifiedName());
    public TScrapbook DeserializeScrapbook<TScrapbook>(string? json, string type)
        where TScrapbook : RScrapbook
    {
        var scrapbookType = Type.GetType(type, throwOnError: true)!;
        if (json == null)
            return (TScrapbook) Activator.CreateInstance(scrapbookType)!;
        
        return (TScrapbook) JsonSerializer.Deserialize(json, scrapbookType)!;
    }

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
        => new(JsonSerializer.Serialize(result), result?.GetType().SimpleQualifiedName());
    public TResult DeserializeResult<TResult>(string json, string type) 
        => (TResult) JsonSerializer.Deserialize(json, Type.GetType(type, throwOnError: true)!)!;

    public JsonAndType SerializeEvent<TEvent>(TEvent @event) where TEvent : notnull 
        => new(JsonSerializer.Serialize(@event), @event.GetType().SimpleQualifiedName());
    public object DeserializeEvent(string json, string type)
        => JsonSerializer.Deserialize(json, Type.GetType(type, throwOnError: true)!)!;
}