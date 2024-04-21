using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage;

public record StoredFunction(
    FunctionId FunctionId,
    StoredParameter Parameter,
    string? DefaultState,
    Status Status,
    StoredResult Result,
    StoredException? Exception,
    long? PostponedUntil,
    int Epoch,
    long LeaseExpiration,
    long Timestamp,
    long InterruptCount
);

public record StoredExecutingFunction(FunctionInstanceId InstanceId, int Epoch, long LeaseExpiration);
public record StoredPostponedFunction(FunctionInstanceId InstanceId, int Epoch, long PostponedUntil);

public record StoredParameter(string ParamJson, string ParamType);
public record StoredResult(string? ResultJson, string? ResultType)
{
    public static StoredResult Null { get; } = new(ResultJson: null, ResultType: null);
}
public record StoredException(string ExceptionMessage, string? ExceptionStackTrace, string ExceptionType);
public record StatusAndEpoch(Status Status, int Epoch);

public record StoredEffect(EffectId EffectId, WorkStatus WorkStatus, string? Result, StoredException? StoredException);
public record StoredState(StateId StateId, string StateJson);

internal static class StorageTypeExtensions
{
    public static TParam Deserialize<TParam>(this StoredParameter parameter, ISerializer serializer) 
        where TParam : notnull 
        => serializer.DeserializeParameter<TParam>(parameter.ParamJson, parameter.ParamType);

    public static TResult? Deserialize<TResult>(this StoredResult result, ISerializer serializer)
        => result.ResultJson == null || result.ResultType == null
            ? default(TResult?) 
            : serializer.DeserializeResult<TResult>(result.ResultJson, result.ResultType);
}