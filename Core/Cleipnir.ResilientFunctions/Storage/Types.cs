using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.Storage;

public record StoredId(StoredInstance Instance)
{
    public override string ToString() => $"{Instance.Value}";

    public StoredType Type => Instance.StoredType;
    
    public static StoredId Deserialize(string s)
    {
        var storedInstance = s.ToGuid().ToStoredInstance();
        return new StoredId(storedInstance);
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
    
    public void Deconstruct(out StoredType type, out StoredInstance instance)
    {
        type = Type;
        instance = Instance;
    }
}
public record StoredType(int Value);

public record StoredInstance(Guid Value, StoredType StoredType)
{
    public static implicit operator StoredInstance(Guid id) => new(id.ToStoredInstance());

    public static StoredInstance Create(Guid id) => id.ToStoredInstance(); 
    
    public static StoredInstance Create(string instanceId, StoredType storedType)
    {
        // Convert the input string to a byte array and compute the hash.
        using var sha256 = SHA256.Create();
        var hash = sha256.ComputeHash(Encoding.UTF8.GetBytes(instanceId));
        
        byte[] guidBytes = new byte[16];
        for (int i = 0; i < 16; i++)
        {
            guidBytes[i] = (byte)(hash[i] ^ hash[i + 16]);
        }
       
        var typeBytes = BitConverter.GetBytes(storedType.Value);
        if (!BitConverter.IsLittleEndian)
            Array.Reverse(typeBytes);
    
        typeBytes.CopyTo(guidBytes, index: 0); // overwrites first 4 bytes
        var id = new Guid(guidBytes);
        return new StoredInstance(id);
    }

    public StoredId ToStoredId() => new StoredId(this);
}

public static class StoredInstanceExtensions
{
    public static StoredInstance ToStoredInstance(this string instanceId, StoredType storedType)
        => StoredInstance.Create(instanceId, storedType);

    public static StoredInstance ToStoredInstance(this FlowInstance instance, StoredType storedType)
        => instance.Value.ToStoredInstance(storedType);

    public static StoredInstance ToStoredInstance(this Guid instanceId)
    {
        var bytes = instanceId.ToByteArray();
        if (!BitConverter.IsLittleEndian)
        {
            var b = bytes[0];
            bytes[0] = bytes[3];
            bytes[3] = b;
            b = bytes[2];
            bytes[2] = bytes[3];
            bytes[3] = b;
        }

        var value = BitConverter.ToInt32(bytes, startIndex: 0);
        var storedType = new StoredType(value);
        return new StoredInstance(instanceId, storedType);
    }
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