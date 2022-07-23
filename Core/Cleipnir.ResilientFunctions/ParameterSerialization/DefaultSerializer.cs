using System;
using System.Text.Json;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.ParameterSerialization;

public class DefaultSerializer : ISerializer
{
    public static readonly DefaultSerializer Instance = new();

    private DefaultSerializer() {}
    
    public string SerializeParameter(object parameter) => JsonSerializer.Serialize(parameter);
    public TParam DeserializeParameter<TParam>(string json, string type)
        => (TParam) JsonSerializer.Deserialize(json, Type.GetType(type, throwOnError: true)!)!;

    public string SerializeScrapbook(RScrapbook scrapbook)
        => JsonSerializer.Serialize(scrapbook, scrapbook.GetType());
    public TScrapbook DeserializeScrapbook<TScrapbook>(string? json, string type)
        where TScrapbook : RScrapbook
    {
        var scrapbookType = Type.GetType(type, throwOnError: true)!;
        if (json == null)
            return (TScrapbook) Activator.CreateInstance(scrapbookType)!;
        
        return (TScrapbook) JsonSerializer.Deserialize(json, scrapbookType)!;
    }

    public string SerializeError(RError error) => JsonSerializer.Serialize(error);
    public RError DeserializeError(string json) => JsonSerializer.Deserialize<RError>(json)!;
    
    public string SerializeResult(object result) => JsonSerializer.Serialize(result);
    public TResult DeserializeResult<TResult>(string json, string type) 
        => (TResult) JsonSerializer.Deserialize(json, Type.GetType(type, throwOnError: true)!)!;
}