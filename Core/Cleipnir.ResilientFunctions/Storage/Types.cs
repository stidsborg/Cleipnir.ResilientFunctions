using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.Storage;

public record StoredId(StoredType Type, StoredInstance Instance)
{
    public override string ToString() => $"{Instance.Value}@{Type.Value}";

    public static StoredId Deserialize(string s)
    {
        var split = s.Split("@");
        return new StoredId(int.Parse(split[1]).ToStoredType(), new StoredInstance(Guid.Parse(split[0])));
    }

    public string Serialize() => ToString();

    public Guid ToGuid()
    {
        var instanceGuid = Instance.Value;
        var instanceBytes = instanceGuid.ToByteArray();
        var typeBytes = BitConverter.GetBytes(Type.Value);
        if (!BitConverter.IsLittleEndian)
            Array.Reverse(typeBytes);
        
        typeBytes.CopyTo(instanceBytes, index: 0); // overwrites first 4 bytes
        var id = new Guid(instanceBytes);
        return id;
    }
}
public record StoredType(int Value);

public record StoredInstance(Guid Value)
{
    public static implicit operator StoredInstance(Guid id) => new(id.ToStoredInstance());
    
    public static StoredInstance Create(string instanceId) 
        => new(StoredIdFactory.FromString(instanceId));
}

public static class StoredInstanceExtensions
{
    public static StoredInstance ToStoredInstance(this string instanceId)
        => StoredInstance.Create(instanceId);

    public static StoredInstance ToStoredInstance(this FlowInstance instance)
        => instance.Value.ToStoredInstance();
    
    public static StoredInstance ToStoredInstance(this Guid instanceId) => new(instanceId);
}

internal static class StoredTypeExtension
{
    public static StoredType ToStoredType(this int storedType) => new(storedType);
};

public record StoredFlow(
    StoredId StoredId,
    string HumanInstanceId,
    byte[]? Parameter,
    Status Status,
    byte[]? Result,
    StoredException? Exception,
    long Expires,
    long Timestamp,
    bool Interrupted,
    StoredId? ParentId,
    ReplicaId? OwnerId,
    StoredType StoredType
);

public record StoredException(string ExceptionMessage, string? ExceptionStackTrace, string ExceptionType);
public record StatusAndId(StoredId StoredId, Status Status, long Expiry);

public record StoredEffectId(Guid Value)
{
    public static StoredEffectId Create(EffectId effectId) 
        => new(StoredIdFactory.FromString(effectId.Serialize()));
}

public static class StoredEffectIdExtensions
{
    public static StoredEffectId ToStoredEffectId(this string effectId, EffectType effectType) => ToStoredEffectId(effectId.ToEffectId(effectType));
    public static StoredEffectId ToStoredEffectId(this EffectId effectId) => StoredEffectId.Create(effectId);
}

public record StoredEffectChange(
    StoredId StoredId,
    StoredEffectId EffectId,
    CrudOperation Operation,
    StoredEffect? StoredEffect)
{
    public static StoredEffectChange CreateDelete(StoredId storedId, StoredEffectId effectId)
        => new(storedId, effectId, CrudOperation.Delete, StoredEffect: null);
}

public enum CrudOperation
{
    Insert,
    Update,
    Delete
}

public record StoredEffect(
    EffectId EffectId,
    StoredEffectId StoredEffectId,
    WorkStatus WorkStatus,
    byte[]? Result,
    StoredException? StoredException
)
{
    public static StoredEffect CreateCompleted(EffectId effectId, byte[] result)
        => new(effectId, effectId.ToStoredEffectId(), WorkStatus.Completed, result, StoredException: null);
    public static StoredEffect CreateCompleted(EffectId effectId)
        => new(effectId, effectId.ToStoredEffectId(), WorkStatus.Completed, Result: null, StoredException: null);
    public static StoredEffect CreateStarted(EffectId effectId)
        => new(effectId, effectId.ToStoredEffectId(), WorkStatus.Started, Result: null, StoredException: null);
    public static StoredEffect CreateFailed(EffectId effectId, StoredException storedException)
        => new(effectId, effectId.ToStoredEffectId(), WorkStatus.Failed, Result: null, storedException);

    public static StoredEffect CreateState(StoredState storedState)
    {
        var effectId = storedState.StateId.Value.ToEffectId(effectType: EffectType.State);
        return new StoredEffect(
            effectId,
            effectId.ToStoredEffectId(),
            WorkStatus.Completed,
            storedState.StateJson,
            StoredException: null
        );
    }
        
};
public record StoredState(StateId StateId, byte[] StateJson);

public record IdWithParam(StoredId StoredId, string HumanInstanceId, byte[]? Param);

public record StoredFlowWithEffectsAndMessages(
    StoredFlow StoredFlow,
    IReadOnlyList<StoredEffect> Effects,
    IReadOnlyList<StoredMessage> Messages
);

public static class StoredEffectExtensions
{
    public static StoredEffectChange ToStoredChange(this StoredEffect effect, StoredId storedId, CrudOperation operation) 
        => new(storedId, effect.StoredEffectId, operation, effect);
}

public record StoredReplica(ReplicaId ReplicaId, long LatestHeartbeat);