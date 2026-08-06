using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Cleipnir.ResilientFunctions.Domain;

internal static class EffectValue
{
    /// <summary>
    /// The value to persist for an effect result, and the type to serialize - and record - it as. The runtime type
    /// of the instance is used rather than the declared one, so a result captured through a base type is restored
    /// as the instance it was.
    ///
    /// A lazily-typed sequence is materialized first: the runtime type of a LINQ query or a yield-return block is a
    /// compiler-generated iterator that serializes fine but can never be deserialized back into - it has no public
    /// constructor. Its materialized form both survives the round-trip and describes the captured value more
    /// precisely than the declared type does (which for Capture&lt;object&gt;(...) describes nothing at all).
    /// </summary>
    public static (object? Value, Type Type) ForSerialization(object? value, Type declaredType)
    {
        if (value == null)
            return (null, declaredType);

        var runtimeType = value.GetType();
        if (!IsLazilyTypedSequence(value, runtimeType))
            return (value, runtimeType);

        var materialized = Materialize((IEnumerable)value, ElementType(runtimeType));
        return (materialized, materialized.GetType());
    }

    // Only invisible sequence types are materialized: they are the ones the compiler and LINQ generate, and no
    // publicly named collection is turned into something the caller did not capture. Dictionaries are left alone
    // because their materialized form (a sequence of key-value pairs) serializes to a different shape than the
    // dictionary itself does.
    private static bool IsLazilyTypedSequence(object value, Type runtimeType)
        => value is IEnumerable and not string
           && !runtimeType.IsVisible
           && value is not IDictionary
           && !ImplementsGenericInterface(runtimeType, typeof(IDictionary<,>));

    private static Type ElementType(Type sequenceType)
        => sequenceType
               .GetInterfaces()
               .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>))
               .Select(i => i.GetGenericArguments()[0])
               .FirstOrDefault()
           ?? typeof(object);

    private static object Materialize(IEnumerable sequence, Type elementType)
    {
        var list = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(elementType))!;
        foreach (var element in sequence)
            list.Add(element);

        return list;
    }

    private static bool ImplementsGenericInterface(Type type, Type genericInterface)
        => type.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == genericInterface);
}
