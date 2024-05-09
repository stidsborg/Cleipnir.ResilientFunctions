using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage;

public record StoredFunction(
    FunctionId FunctionId,
    string? Parameter,
    string? DefaultState,
    Status Status,
    string? Result,
    StoredException? Exception,
    long? PostponedUntil,
    int Epoch,
    long LeaseExpiration,
    long Timestamp,
    long InterruptCount
);

public record StoredExecutingFunction(FunctionInstanceId InstanceId, int Epoch, long LeaseExpiration);
public record StoredPostponedFunction(FunctionInstanceId InstanceId, int Epoch, long PostponedUntil);

public record StoredException(string ExceptionMessage, string? ExceptionStackTrace, string ExceptionType);
public record StatusAndEpoch(Status Status, int Epoch);

public record StoredEffect(EffectId EffectId, WorkStatus WorkStatus, string? Result, StoredException? StoredException);
public record StoredState(StateId StateId, string StateJson);