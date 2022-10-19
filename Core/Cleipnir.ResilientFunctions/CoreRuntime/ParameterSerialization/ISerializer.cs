using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;

public interface ISerializer
{
    StoredParameter SerializeParameter<TParam>(TParam parameter) where TParam : notnull;
    TParam DeserializeParameter<TParam>(string json, string type) where TParam : notnull;
    StoredScrapbook SerializeScrapbook<TScrapbook>(TScrapbook scrapbook) where TScrapbook : RScrapbook;
    TScrapbook DeserializeScrapbook<TScrapbook>(string json, string type) where TScrapbook : RScrapbook;
    string SerializeError(RError error);
    RError DeserializeError(string json);
    StoredResult SerializeResult<TResult>(TResult result);
    TResult DeserializeResult<TResult>(string json, string type);
}