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
/// Registry-wide cache over <see cref="ITypeStore"/> mapping the .NET types persisted inside effect results and
/// messages to the ids they are persisted under. A type is encoded as the UTF-8 bytes of its
/// <see cref="TypeHelper.SimpleQualifiedName"/> and its id is content-derived - the first 8 bytes of the SHA-256
/// hash of the encoded type - so <see cref="GetTypeId"/> computes it without touching the store; only the
/// id -> encoded-type row must be persisted before the first durable write referencing it, as a payload whose
/// type cannot be looked up can never be deserialized. Every durable write path therefore awaits
/// <see cref="EnsurePersisted"/> first: it persists every id minted by this process that is not yet known to be
/// durable - including ids buried inside already-encoded payloads - and completes synchronously once all minted
/// types are persisted.
/// </summary>
public class TypeMapper(ITypeStore typeStore)
{
    private readonly ConcurrentDictionary<Type, TypeId> _typeIds = new();
    // Only ever contains mappings that are durable in the type store: an entry is added after its insert has
    // completed (or from a store refresh) - never before. Concurrent EnsurePersisted calls may therefore insert
    // the same mapping more than once; inserts are idempotent, so no queuing is needed.
    private readonly ConcurrentDictionary<TypeId, byte[]> _serializedTypes = new();

    public TypeId GetTypeId(Type type)
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

    public Task EnsurePersisted()
    {
        Dictionary<TypeId, byte[]>? missing = null;
        foreach (var (type, typeId) in _typeIds)
            if (!_serializedTypes.ContainsKey(typeId))
                (missing ??= new Dictionary<TypeId, byte[]>())[typeId] = type.SerializeType();

        return missing == null
            ? Task.CompletedTask
            : PersistMissing(missing);
    }

    private async Task PersistMissing(Dictionary<TypeId, byte[]> missing)
    {
        await typeStore.InsertTypes(missing);
        foreach (var (typeId, serializedType) in missing)
            _serializedTypes.TryAdd(typeId, serializedType);
    }

    public Type ResolveType(TypeId typeId)
    {
        var serializedType = GetSerializedType(typeId);
        return serializedType.ResolveType()
            ?? throw new TypeLoadException(
                $"Type '{serializedType.ToStringFromUtf8Bytes()}' with id '{typeId}' could not be resolved"
            );
    }

    private byte[] GetSerializedType(TypeId typeId)
    {
        if (_serializedTypes.TryGetValue(typeId, out var serializedType))
            return serializedType;

        // A minted-but-not-yet-persisted id occurs when a payload created in this process is read back before
        // its first durable write.
        if (FindMintedSerializedType(typeId) is { } mintedSerializedType)
            return mintedSerializedType;

        // An unknown id belongs to a payload persisted by a process whose type mappings were stored before the
        // payload was, so a refresh is guaranteed to surface it. Blocking is accepted here: resolution happens
        // inside synchronous deserialization paths and a given type is only ever fetched once per process.
        RefreshFromStore().GetAwaiter().GetResult();

        if (_serializedTypes.TryGetValue(typeId, out serializedType))
            return serializedType;

        throw new TypeLoadException($"Type with id '{typeId}' was not found in the type store");
    }

    // Reverse lookup over the ids minted by GetTypeId - a handful of entries, and only consulted until the
    // mapping in question has been persisted.
    private byte[]? FindMintedSerializedType(TypeId typeId)
    {
        foreach (var (type, id) in _typeIds)
            if (id == typeId)
                return type.SerializeType();

        return null;
    }

    private async Task RefreshFromStore()
    {
        foreach (var (typeId, serializedType) in await typeStore.GetAllTypes())
            _serializedTypes.TryAdd(typeId, serializedType);
    }

    public static TypeId CalculateTypeId(byte[] serializedType)
        => new(BinaryPrimitives.ReadInt64LittleEndian(SHA256.HashData(serializedType)));
}
