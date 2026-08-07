using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

/// <summary>
/// Registry-wide cache over <see cref="ITypeStore"/> mapping the .NET types effect results are serialized as to
/// the ids they are persisted under. A type is encoded as the UTF-8 bytes of its
/// <see cref="TypeHelper.SimpleQualifiedName"/> and its id is content-derived - the first 8 bytes of the SHA-256
/// hash of the encoded type - so <see cref="GetTypeId"/> computes it without touching the store; only the
/// id -> encoded-type row must be persisted before the first effect referencing it, as an effect whose result
/// type cannot be looked up can never be deserialized. Every effect write path therefore awaits
/// <see cref="EnsurePersisted"/> before handing effects to the store; the call completes synchronously once the
/// types involved are known to be persisted.
/// </summary>
public class TypeMapper(ITypeStore typeStore)
{
    private readonly ConcurrentDictionary<Type, long> _typeIds = new();
    // Only ever contains mappings that are durable in the type store: an entry is added after its insert has
    // completed (or from a store refresh) - never before. Concurrent EnsurePersisted calls may therefore insert
    // the same mapping more than once; inserts are idempotent, so no queuing is needed.
    private readonly ConcurrentDictionary<long, byte[]> _serializedTypes = new();

    public long GetTypeId(Type type)
    {
        if (_typeIds.TryGetValue(type, out var cachedTypeId))
            return cachedTypeId;

        var serializedType = type.SerializeType();
        var typeId = CalculateTypeId(serializedType);

        if (_serializedTypes.TryGetValue(typeId, out var existing) && !existing.SequenceEqual(serializedType))
            throw new InvalidOperationException(
                $"Type id '{typeId}' of type '{serializedType.ToStringFromUtf8Bytes()}' collides with already-registered type '{existing.ToStringFromUtf8Bytes()}'"
            );

        _typeIds.TryAdd(type, typeId);
        return typeId;
    }

    public Task EnsurePersisted(IReadOnlyList<long> typeIds)
    {
        List<long>? missing = null;
        foreach (var typeId in typeIds)
            if (!_serializedTypes.ContainsKey(typeId))
                (missing ??= []).Add(typeId);

        return missing == null
            ? Task.CompletedTask
            : PersistMissing(missing);
    }

    private async Task PersistMissing(List<long> missing)
    {
        var toInsert = new Dictionary<long, byte[]>();
        List<long>? unknown = null;
        foreach (var typeId in missing)
            if (FindMintedSerializedType(typeId) is { } serializedType)
                toInsert[typeId] = serializedType;
            else
                (unknown ??= []).Add(typeId);

        if (toInsert.Count > 0)
        {
            await typeStore.InsertTypes(toInsert);
            foreach (var (typeId, serializedType) in toInsert)
                _serializedTypes.TryAdd(typeId, serializedType);
        }

        if (unknown != null)
        {
            // An id this process never minted was read from the store (an existing effect being re-persisted),
            // so its mapping must already be there - verify against the store instead of inventing a row whose
            // content is not known.
            await RefreshFromStore();
            foreach (var typeId in unknown)
                if (!_serializedTypes.ContainsKey(typeId))
                    throw new InvalidOperationException($"Type with id '{typeId}' referenced by an effect was not found in the type store");
        }
    }

    // Reverse lookup over the ids minted by GetTypeId - a handful of entries, and only consulted until the
    // mapping in question has been persisted.
    private byte[]? FindMintedSerializedType(long typeId)
    {
        foreach (var (type, id) in _typeIds)
            if (id == typeId)
                return type.SerializeType();

        return null;
    }

    public Type ResolveType(long typeId)
    {
        var serializedType = GetSerializedType(typeId);
        return serializedType.ResolveType()
            ?? throw new TypeLoadException(
                $"Type '{serializedType.ToStringFromUtf8Bytes()}' with id '{typeId}' could not be resolved"
            );
    }

    private byte[] GetSerializedType(long typeId)
    {
        if (_serializedTypes.TryGetValue(typeId, out var serializedType))
            return serializedType;

        // A minted-but-not-yet-persisted id occurs when an effect created in this process is read back before
        // its first flush.
        if (FindMintedSerializedType(typeId) is { } mintedSerializedType)
            return mintedSerializedType;

        // An unknown id belongs to an effect persisted by a process whose type mappings were stored before the
        // effect was, so a refresh is guaranteed to surface it. Blocking is accepted here: resolution happens
        // inside synchronous deserialization paths and a given type is only ever fetched once per process.
        RefreshFromStore().GetAwaiter().GetResult();

        if (_serializedTypes.TryGetValue(typeId, out serializedType))
            return serializedType;

        throw new TypeLoadException($"Type with id '{typeId}' was not found in the type store");
    }

    private async Task RefreshFromStore()
    {
        foreach (var (typeId, serializedType) in await typeStore.GetAllTypes())
            _serializedTypes.TryAdd(typeId, serializedType);
    }

    public static long CalculateTypeId(byte[] serializedType)
        => BinaryPrimitives.ReadInt64LittleEndian(SHA256.HashData(serializedType));
}
