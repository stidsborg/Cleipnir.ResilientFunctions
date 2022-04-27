using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.ParameterSerialization;

public interface ISerializer
{
    string SerializeParameter(object parameter);
    object DeserializeParameter(string json, string type);
    string SerializeScrapbook(RScrapbook scrapbook);
    RScrapbook DeserializeScrapbook(string? json, string type);
    string SerializeError(RError error);
    RError DeserializeError(string json);
    string SerializeResult(object result);
    object DeserializeResult(string json, string type);
}