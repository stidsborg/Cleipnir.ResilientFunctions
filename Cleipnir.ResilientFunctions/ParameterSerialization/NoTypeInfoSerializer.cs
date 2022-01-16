using System;
using System.Runtime.Serialization;
using Newtonsoft.Json;

namespace Cleipnir.ResilientFunctions.ParameterSerialization;

public class NoTypeInfoSerializer : ISerializer, IDeserializer
{
    public static NoTypeInfoSerializer Instance { get; } = new();
    
    public string Serialize(object parameter, ParameterType parameterType, RFuncType rFuncType) 
        => JsonConvert.SerializeObject(parameter);

    public object DeserializeParameter(
        string json,
        string storedParameterType,
        ParameterType parameterType,
        RFuncType rFuncType
    ) => parameterType switch
    {
        ParameterType.InputParameter => JsonConvert.DeserializeObject(json, rFuncType.ParameterType),
        ParameterType.Scrapbook => JsonConvert.DeserializeObject(json, rFuncType.ScrapbookType!),
        ParameterType.ReturnValue => JsonConvert.DeserializeObject(json, rFuncType.ReturnValueType!),
        _ => throw new ArgumentOutOfRangeException(nameof(parameterType), parameterType, null)
    } ?? throw new SerializationException($"Parameter '{parameterType}' was deserialized to null");
}