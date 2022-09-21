using System;
using System.Collections.Generic;
using System.Text;
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

    public string SerializeScrapbooks(IEnumerable<OwnedScrapbook> scrapbooks)
    {
        var stringBuilder = new StringBuilder("{");
        foreach (var (owner, scrapbook, type) in scrapbooks)
            stringBuilder.Append($@"""{owner}"": {{ ""Type"": ""{ type }"", ""Json"": { JsonSerializer.Serialize(scrapbook) } }},");

        stringBuilder.Length--; //remove last comma - only needed between blocks of json
        stringBuilder.Append('}');
        return stringBuilder.ToString();
    }
    public Dictionary<string, RScrapbook> DeserializeScrapbooks(string json)
    {
        var toReturn = new Dictionary<string, RScrapbook>();
        var dict = JsonSerializer.Deserialize<Dictionary<string, TypeAndJsonElement>>(json)!;
        foreach (var (owner, (type, jsonElement)) in dict)
            toReturn[owner] = (RScrapbook)jsonElement.Deserialize(Type.GetType(type, throwOnError: true)!)!;

        return toReturn;
    }

    public string SerializeError(RError error) => JsonSerializer.Serialize(error);
    public RError DeserializeError(string json) => JsonSerializer.Deserialize<RError>(json)!;
    
    public string SerializeResult(object result) => JsonSerializer.Serialize(result);
    public TResult DeserializeResult<TResult>(string json, string type) 
        => (TResult) JsonSerializer.Deserialize(json, Type.GetType(type, throwOnError: true)!)!;
}