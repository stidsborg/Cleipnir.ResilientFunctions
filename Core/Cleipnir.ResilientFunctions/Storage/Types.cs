using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage.Session;

namespace Cleipnir.ResilientFunctions.Storage;

public record StoredId(Guid AsGuid)
{
    public override string ToString() => $"{AsGuid}";

    public StoredType Type => ExtractStoredType();

    public static StoredId Deserialize(string s) => new(Guid.Parse(s));

    public string Serialize() => ToString();

    public static Guid Create(StoredType type, Guid id)
    {
        var instanceBytes = id.ToByteArray();
        var typeBytes = BitConverter.GetBytes(type.Value);
        if (!BitConverter.IsLittleEndian)
            Array.Reverse(typeBytes);
        
        typeBytes.CopyTo(instanceBytes, index: 0); // overwrites first 2 bytes
        return new Guid(instanceBytes);
    }
    
    public static StoredId Create(StoredType type, string instance)
    {
        // Convert the input string to a byte array and compute the hash.
        using var sha256 = SHA256.Create();
        var hash = sha256.ComputeHash(Encoding.UTF8.GetBytes(instance));
    
        var guidBytes = new byte[16];
        for (int i = 0; i < 16; i++)
        {
            guidBytes[i] = (byte)(hash[i] ^ hash[i + 16]);
        }
   
        var typeBytes = BitConverter.GetBytes(type.Value);
        if (!BitConverter.IsLittleEndian)
            Array.Reverse(typeBytes);

        typeBytes.CopyTo(guidBytes, index: 0); // overwrites first 2 bytes
        var id = new Guid(guidBytes);
        return new StoredId(id);
    }

    private StoredType ExtractStoredType()
    {
        var bytes = AsGuid.ToByteArray();
        if (!BitConverter.IsLittleEndian)
            (bytes[0], bytes[1]) = (bytes[1], bytes[0]);

        var type = BitConverter.ToUInt16(bytes, startIndex: 0);
        return type.ToStoredType();
    }

    public ulong AsULong
    {
        get
        {
            var bytes = AsGuid.ToByteArray();

            for (var i = 0; i < 8; i++)
                bytes[i] = (byte)(bytes[i] ^ bytes[i + 8]);

            return BitConverter.ToUInt64(bytes, startIndex: 0);            
        }
    }
}

public static class StoredIdExtensions
{
    internal static StoredId ToStoredId(this Guid id) => new(id);
    internal static StoredId ToStoredId(this string instance, StoredType type) => StoredId.Create(type, instance);
}

public record StoredType(ushort Value);

internal static class StoredTypeExtension
{
    public static StoredType ToStoredType(this ushort storedType) => new(storedType);
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
    EffectId EffectId,
    CrudOperation Operation,
    StoredEffect? StoredEffect)
{
    public static StoredEffectChange CreateDelete(StoredId storedId, EffectId effectId)
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
    WorkStatus WorkStatus,
    byte[]? Result,
    StoredException? StoredException
)
{
    public StoredEffectId StoredEffectId => EffectId.ToStoredEffectId();

    public static StoredEffect CreateCompleted(EffectId effectId, byte[] result)
        => new(effectId, WorkStatus.Completed, result, StoredException: null);
    public static StoredEffect CreateCompleted(EffectId effectId)
        => new(effectId, WorkStatus.Completed, Result: null, StoredException: null);
    public static StoredEffect CreateStarted(EffectId effectId)
        => new(effectId, WorkStatus.Started, Result: null, StoredException: null);
    public static StoredEffect CreateFailed(EffectId effectId, StoredException storedException)
        => new(effectId, WorkStatus.Failed, Result: null, storedException);

    public static StoredEffect CreateState(StoredState storedState)
    {
        var effectId = storedState.StateId.Value.ToEffectId(effectType: EffectType.State);
        return new StoredEffect(
            effectId,
            WorkStatus.Completed,
            storedState.StateJson,
            StoredException: null
        );
    }
};
public record StoredEffectWithPosition(StoredEffect Effect, long Position);

public record StoredState(StateId StateId, byte[] StateJson);

public record IdWithParam(StoredId StoredId, string HumanInstanceId, byte[]? Param);

public record StoredFlowWithEffectsAndMessages(
    StoredFlow StoredFlow,
    IReadOnlyList<StoredEffect> Effects,
    IReadOnlyList<StoredMessage> Messages,
    IStorageSession StorageSession
);

public static class StoredEffectExtensions
{
    public static StoredEffectChange ToStoredChange(this StoredEffect effect, StoredId storedId, CrudOperation operation)
        => new(storedId, effect.EffectId, operation, effect);
}

public record StoredReplica(ReplicaId ReplicaId, long LatestHeartbeat);