using System;
using Cleipnir.ResilientFunctions.Domain;
using Newtonsoft.Json;

namespace Cleipnir.ResilientFunctions.ParameterSerialization;

public class DefaultSerializer : ISerializer
{
    public static readonly DefaultSerializer Instance = new(); 
    private static JsonSerializerSettings SerializerSettings { get; } = new() { TypeNameHandling = TypeNameHandling.Auto };
    
    private DefaultSerializer() {}
    
    public string SerializeParameter(object parameter)
        => JsonConvert.SerializeObject(parameter, SerializerSettings);

    public object DeserializeParameter(string json, string type)
        => JsonConvert.DeserializeObject(json, Type.GetType(type, throwOnError: true)!)!;

    public string SerializeScrapbook(RScrapbook scrapbook)
        => JsonConvert.SerializeObject(scrapbook, SerializerSettings);

    public RScrapbook DeserializeScrapbook(string? json, string type)
    {
        var scrapbookType = Type.GetType(type, throwOnError: true)!;
        if (json == null)
            return (RScrapbook) Activator.CreateInstance(scrapbookType)!;
        
        return (RScrapbook) JsonConvert.DeserializeObject(json, scrapbookType)!;
    }

    public string SerializeError(RError error)
        => JsonConvert.SerializeObject(error, SerializerSettings);

    public RError DeserializeError(string json)
        => JsonConvert.DeserializeObject<RError>(json)!;

    public string SerializeResult(object result)
        => JsonConvert.SerializeObject(result, SerializerSettings);

    public object DeserializeResult(string json, string type)
        =>JsonConvert.DeserializeObject(json, Type.GetType(type, throwOnError: true)!)!;
}