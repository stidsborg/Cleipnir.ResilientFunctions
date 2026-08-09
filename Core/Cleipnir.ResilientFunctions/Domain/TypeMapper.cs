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
    // Minted-but-not-yet-persisted mappings: GetTypeId adds, EnsurePersisted drains once the insert has
    // completed - so its fast path is an emptiness check rather than a sweep over every minted type.
    private readonly ConcurrentDictionary<TypeId, byte[]> _unpersisted = new();
    // Resolved id -> Type cache: populated by GetTypeId (the Type is in hand when minting) and by the first
    // resolution of a foreign id, so repeated resolves skip the Type.GetType round-trip.
    private readonly ConcurrentDictionary<TypeId, Type> _resolvedTypes = new();

    public TypeId GetTypeId(Type type)
    {
        if (_typeIds.TryGetValue(type, out var cachedTypeId))
            return cachedTypeId;

        var serializedType = type.SerializeType();
        var typeId = CalculateTypeId(serializedType);

        if (_serializedTypes.TryGetValue(typeId, out var existing))
        {
            if (!existing.SequenceEqual(serializedType))
                throw new InvalidOperationException(
                    $"Type id '{typeId}' of type '{serializedType.ToStringFromUtf8Bytes()}' collides with already-registered type '{existing.ToStringFromUtf8Bytes()}'"
                );
        }
        else
            _unpersisted.TryAdd(typeId, serializedType);

        _resolvedTypes.TryAdd(typeId, type);
        _typeIds.TryAdd(type, typeId);
        return typeId;
    }

    public Task EnsurePersisted()
        => _unpersisted.IsEmpty
            ? Task.CompletedTask
            : PersistMissing();

    private async Task PersistMissing()
    {
        var missing = _unpersisted.ToDictionary(kv => kv.Key, kv => kv.Value);
        await typeStore.InsertTypes(missing);
        foreach (var (typeId, serializedType) in missing)
        {
            _serializedTypes.TryAdd(typeId, serializedType);
            _unpersisted.TryRemove(typeId, out _);
        }
    }

    /// <summary>
    /// The .NET type the id was minted for. Completes synchronously for an already-resolved id - which every id
    /// this process minted is, and every foreign id becomes after its first resolution - so only the first
    /// resolution of a type persisted by another process reaches the store.
    /// </summary>
    public Task<Type> ResolveType(TypeId typeId)
        => _resolvedTypes.TryGetValue(typeId, out var resolvedType)
            ? resolvedType.ToTask()
            : ResolveUncachedType(typeId);

    private async Task<Type> ResolveUncachedType(TypeId typeId)
    {
        var serializedType = await GetSerializedType(typeId);
        var type = serializedType.ResolveType()
            ?? throw new TypeLoadException(
                $"Type '{serializedType.ToStringFromUtf8Bytes()}' with id '{typeId}' could not be resolved"
            );

        _resolvedTypes.TryAdd(typeId, type);
        return type;
    }

    private async Task<byte[]> GetSerializedType(TypeId typeId)
    {
        if (_serializedTypes.TryGetValue(typeId, out var serializedType))
            return serializedType;

        // A minted-but-not-yet-persisted id occurs when a payload created in this process is read back before
        // its first durable write.
        if (_unpersisted.TryGetValue(typeId, out var unpersistedType))
            return unpersistedType;

        // An unknown id belongs to a payload persisted by a process whose type mappings were stored before the
        // payload was, so a refresh is guaranteed to surface it.
        await RefreshFromStore();

        if (_serializedTypes.TryGetValue(typeId, out serializedType))
            return serializedType;

        throw new TypeLoadException($"Type with id '{typeId}' was not found in the type store");
    }

    private async Task RefreshFromStore()
    {
        foreach (var (typeId, serializedType) in await typeStore.GetAllTypes())
            _serializedTypes.TryAdd(typeId, serializedType);
    }

    public static TypeId CalculateTypeId(byte[] serializedType)
        => new(BinaryPrimitives.ReadInt64LittleEndian(SHA256.HashData(serializedType)));
}
