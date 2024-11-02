using System;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage;

public record StoredId(StoredType Type, string Instance);
public record StoredType(int Value);

public record StoredInstance(Guid Value)
{
    public static StoredInstance Create(string instanceId) 
        => new(InstanceIdFactory.FromString(instanceId));
    
    public static StoredInstance Create(int instanceId) 
        => new(InstanceIdFactory.FromInt(instanceId));
    
    public static StoredInstance Create(long instanceId) 
        => new(InstanceIdFactory.FromLong(instanceId));
}

internal static class StoredTypeExtension
{
    public static StoredType ToStoredType(this int storedType) => new(storedType);
};

public record StoredFlow(
    StoredId FlowId,
    byte[]? Parameter,
    Status Status,
    byte[]? Result,
    StoredException? Exception,
    int Epoch,
    long Expires,
    long Timestamp,
    bool Interrupted
);

public record IdAndEpoch(StoredId FlowId, int Epoch);

public record StoredException(string ExceptionMessage, string? ExceptionStackTrace, string ExceptionType);
public record StatusAndEpoch(Status Status, int Epoch);

public record StoredEffect(
    EffectId EffectId,
    bool IsState,
    WorkStatus WorkStatus,
    byte[]? Result,
    StoredException? StoredException
)
{
    public static StoredEffect CreateCompleted(EffectId effectId, byte[] result)
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
public record StoredState(StateId StateId, byte[] StateJson);

public record IdWithParam(StoredId StoredId, byte[]? Param);