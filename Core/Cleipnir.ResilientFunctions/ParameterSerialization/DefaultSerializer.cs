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

    public string SerializeScrapbook(RScrapbook scrapbook)
        => JsonSerializer.Serialize(scrapbook, scrapbook.GetType());

    public RScrapbook DeserializeScrapbook(string? json, string type)
    {
        var scrapbookType = Type.GetType(type, throwOnError: true)!;
        if (json == null)
            return (RScrapbook) Activator.CreateInstance(scrapbookType)!;
        
        return (RScrapbook) JsonSerializer.Deserialize(json, scrapbookType)!;
    }

    public string SerializeError(RError error) => JsonSerializer.Serialize(error);
    public RError DeserializeError(string json) => JsonSerializer.Deserialize<RError>(json)!;
    public string SerializeResult(object result) => JsonSerializer.Serialize(result);
    public object DeserializeResult(string json, string type) 
        => JsonSerializer.Deserialize(json, Type.GetType(type, throwOnError: true)!)!;
}