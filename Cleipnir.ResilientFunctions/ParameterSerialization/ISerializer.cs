using System;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.ParameterSerialization;

public interface ISerializer
{
    string SerializeParameter(object parameter);
    object DeserializeParameter(string json, string type);
    string SerializeScrapbook(RScrapbook scrapbook);
    RScrapbook DeserializeScrapbook(string? json, string type);
    string SerializeFault(Exception fault);
    Exception DeserializeFault(string json, string type);
    string SerializeResult(object result);
    object DeserializeResult(string json, string type);
}