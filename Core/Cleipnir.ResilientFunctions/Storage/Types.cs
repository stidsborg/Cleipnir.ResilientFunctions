using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.ParameterSerialization;

namespace Cleipnir.ResilientFunctions.Storage;

public record StoredFunction(
    FunctionId FunctionId,
    StoredParameter Parameter,
    StoredScrapbook? Scrapbook,
    Status Status,
    StoredResult? Result,
    string? ErrorJson,
    long? PostponedUntil,
    int Epoch,
    int SignOfLife
);

public record StoredExecutingFunction(FunctionInstanceId InstanceId, int Epoch, int SignOfLife);
public record StoredPostponedFunction(FunctionInstanceId InstanceId, int Epoch, long PostponedUntil);

public record StoredParameter(string ParamJson, string ParamType);
public record StoredResult(string? ResultJson, string? ResultType);
public record StoredScrapbook(string? ScrapbookJson, string ScrapbookType);

internal static class StorageTypeExtensions
{
    public static object Deserialize(this StoredParameter parameter, ISerializer serializer)
        => serializer.DeserializeParameter(parameter.ParamJson, parameter.ParamType);
        
    public static Scrapbook Deserialize(this StoredScrapbook scrapbook, ISerializer serializer)
        => serializer.DeserializeScrapbook(scrapbook.ScrapbookJson!, scrapbook.ScrapbookType);

    public static object? Deserialize(this StoredResult result, ISerializer serializer)
        => result.ResultJson == null || result.ResultType == null
            ? null 
            : serializer.DeserializeResult(result.ResultJson, result.ResultType);
}