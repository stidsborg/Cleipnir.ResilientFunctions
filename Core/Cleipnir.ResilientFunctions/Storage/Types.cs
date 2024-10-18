using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage;

public record StoredFlow(
    FlowId FlowId,
    string? Parameter,
    string? DefaultState,
    Status Status,
    string? Result,
    StoredException? Exception,
    int Epoch,
    long Expires,
    long Timestamp,
    bool Interrupted
);

public record IdAndEpoch(FlowId FlowId, int Epoch);

public record StoredException(string ExceptionMessage, string? ExceptionStackTrace, string ExceptionType);
public record StatusAndEpoch(Status Status, int Epoch);

public record StoredEffect(
    EffectId EffectId,
    bool IsState,
    WorkStatus WorkStatus,
    string? Result,
    StoredException? StoredException
)
{
    public static StoredEffect CreateCompleted(EffectId effectId, string result)
        => new(effectId, IsState: false, WorkStatus.Completed, result, StoredException: null);
    public static StoredEffect CreateCompleted(EffectId effectId)
        => new(effectId, IsState: false, WorkStatus.Completed, Result: null, StoredException: null);
    public static StoredEffect CreateStarted(EffectId effectId)
        => new(effectId, IsState: false, WorkStatus.Started, Result: null, StoredException: null);
    public static StoredEffect CreateFailed(EffectId effectId, StoredException storedException)
        => new(effectId, IsState: false, WorkStatus.Failed, Result: null, storedException);
    public static StoredEffect CreateState(StoredState storedState)
        => new(storedState.StateId.Value, IsState: true, WorkStatus.Completed, storedState.StateJson, StoredException: null);
};
public record StoredState(StateId StateId, string StateJson);

public record IdWithParam(FlowId FlowId, string? Param);