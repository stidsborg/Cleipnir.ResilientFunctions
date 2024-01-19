using System.Collections.Generic;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.Storage;

public record StoredFunction(
    FunctionId FunctionId,
    StoredParameter Parameter,
    StoredScrapbook Scrapbook,
    Status Status,
    StoredResult Result,
    StoredException? Exception,
    long? PostponedUntil,
    int Epoch,
    long LeaseExpiration,
    long Timestamp
);


public record StoredExecutingFunction(FunctionInstanceId InstanceId, int Epoch, long LeaseExpiration);
public record StoredPostponedFunction(FunctionInstanceId InstanceId, int Epoch, long PostponedUntil);

public record StoredParameter(string ParamJson, string ParamType);
public record StoredResult(string? ResultJson, string? ResultType)
{
    public static StoredResult Null { get; } = new(ResultJson: null, ResultType: null);
};
public record StoredScrapbook(string ScrapbookJson, string ScrapbookType);
public record StoredException(string ExceptionMessage, string? ExceptionStackTrace, string ExceptionType);
public record StatusAndEpoch(Status Status, int Epoch);

public record StoredActivity(string ActivityId, WorkStatus WorkStatus, string? Result, StoredException? StoredException);

internal static class StorageTypeExtensions
{
    public static TParam Deserialize<TParam>(this StoredParameter parameter, ISerializer serializer) 
        where TParam : notnull 
        => serializer.DeserializeParameter<TParam>(parameter.ParamJson, parameter.ParamType);
        
    public static TScrapbook Deserialize<TScrapbook>(this StoredScrapbook scrapbook, ISerializer serializer)
        where TScrapbook : RScrapbook
        => serializer.DeserializeScrapbook<TScrapbook>(scrapbook.ScrapbookJson!, scrapbook.ScrapbookType);

    public static TResult? Deserialize<TResult>(this StoredResult result, ISerializer serializer)
        => result.ResultJson == null || result.ResultType == null
            ? default(TResult?) 
            : serializer.DeserializeResult<TResult>(result.ResultJson, result.ResultType);
}