using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.ParameterSerialization;

public interface ISerializer
{
    string SerializeParameter(object parameter);
    object DeserializeParameter(string json, string type);
    string SerializeScrapbook(Scrapbook scrapbook);
    Scrapbook DeserializeScrapbook(string? json, string type);
    string SerializeError(Error error);
    Error DeserializeError(string json);
    string SerializeResult(object result);
    object DeserializeResult(string json, string type);
}