using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

public static class TypeIdTestHelper
{
    /// <summary>
    /// Mints the type's id and persists the id -> encoded-type mapping to the store's type store - what the
    /// runtime's write paths do before persisting a payload - so a message or effect constructed directly in a
    /// test can be resolved after a fetch.
    /// </summary>
    public static TypeId GetTypeId(this IFunctionStore functionStore, Type type)
    {
        var typeMapper = new TypeMapper(functionStore.TypeStore);
        var typeId = typeMapper.GetTypeId(type);
        typeMapper.EnsurePersisted().GetAwaiter().GetResult();
        return typeId;
    }

    public static TypeMapper CreateTypeMapper(this IFunctionStore functionStore)
        => new(functionStore.TypeStore);

    // Blocking, like GetTypeId above: assertion sites read a fetched message inline - often inside a LINQ
    // predicate - and a test thread parked on the type store costs nothing.
    public static object DefaultDeserialize(this StoredMessage message, IFunctionStore functionStore)
        => message.DefaultDeserialize(functionStore.CreateTypeMapper()).GetAwaiter().GetResult();

    public static object DefaultDeserialize(this StoredDlqMessage message, IFunctionStore functionStore)
        => message.DefaultDeserialize(functionStore.CreateTypeMapper()).GetAwaiter().GetResult();

    /// <summary>
    /// The .NET type an effect's result was serialized as, resolved through a fresh mapper: the lookup goes via
    /// the type store, verifying the id -> type mapping was actually persisted alongside the effect.
    /// </summary>
    public static Task<Type> ResolveResultType(this StoredEffect storedEffect, IFunctionStore functionStore)
        => storedEffect.ResolveResultType(functionStore.CreateTypeMapper());
}
