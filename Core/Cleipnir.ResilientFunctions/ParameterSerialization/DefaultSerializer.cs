using System;
using System.Text.Json;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.ParameterSerialization;

public class DefaultSerializer : ISerializer
{
    public static readonly DefaultSerializer Instance = new();

    private DefaultSerializer() {}
    
    public string SerializeParameter(object parameter) => JsonSerializer.Serialize(parameter);

    public object DeserializeParameter(string json, string type)
        => JsonSerializer.Deserialize(json, Type.GetType(type, throwOnError: true)!)!;

    public string SerializeScrapbook(Scrapbook scrapbook)
        => JsonSerializer.Serialize(scrapbook, scrapbook.GetType());

    public Scrapbook DeserializeScrapbook(string? json, string type)
    {
        var scrapbookType = Type.GetType(type, throwOnError: true)!;
        if (json == null)
            return (Scrapbook) Activator.CreateInstance(scrapbookType)!;
        
        return (Scrapbook) JsonSerializer.Deserialize(json, scrapbookType)!;
    }

    public string SerializeError(Error error) => JsonSerializer.Serialize(error);
    public Error DeserializeError(string json) => JsonSerializer.Deserialize<Error>(json)!;
    public string SerializeResult(object result) => JsonSerializer.Serialize(result);
    public object DeserializeResult(string json, string type) 
        => JsonSerializer.Deserialize(json, Type.GetType(type, throwOnError: true)!)!;
}