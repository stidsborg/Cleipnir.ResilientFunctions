using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

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
    /// A non-visible dictionary is materialized into Dictionary&lt;,&gt; rather than a list of pairs - a pair-list
    /// serializes as a JSON array while the dictionary it came from serializes as a JSON object.
    /// </summary>
    public static (object? Value, Type Type) ForSerialization(object? value, Type declaredType)
    {
        if (value == null)
            return (null, declaredType);

        var runtimeType = value.GetType();
        if (!ShouldMaterialize(value, runtimeType))
            return (value, runtimeType);

        var materialized = Materialize((IEnumerable)value, runtimeType);
        return (materialized, materialized.GetType());
    }

    // Only invisible sequence types are materialized: they are the ones the compiler and LINQ generate, and no
    // publicly named collection is turned into something the caller did not capture. A dictionary implementing
    // only the non-generic IDictionary is left alone - without key and value types there is nothing to rebuild
    // it as.
    private static bool ShouldMaterialize(object value, Type runtimeType)
        => value is IEnumerable and not string
           && !runtimeType.IsVisible
           && (value is not IDictionary || DictionaryInterface(runtimeType) != null);

    private static object Materialize(IEnumerable sequence, Type runtimeType)
    {
        var dictionaryInterface = DictionaryInterface(runtimeType);
        return dictionaryInterface == null
            ? MaterializeList(sequence, ElementType(runtimeType))
            : MaterializeDictionary(sequence, dictionaryInterface.GetGenericArguments());
    }

    private static Type? DictionaryInterface(Type type)
        => GenericInterface(type, typeof(IDictionary<,>)) ?? GenericInterface(type, typeof(IReadOnlyDictionary<,>));

    private static Type ElementType(Type sequenceType)
        => GenericInterface(sequenceType, typeof(IEnumerable<>))?.GetGenericArguments()[0] ?? typeof(object);

    private static object MaterializeList(IEnumerable sequence, Type elementType)
    {
        var list = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(elementType))!;
        foreach (var element in sequence)
            list.Add(element);

        return list;
    }

    // Both dictionary interfaces extend IEnumerable<KeyValuePair<K,V>>, so the sequence is enumerated through
    // the generic interface - pairs arrive typed rather than as DictionaryEntry.
    private static object MaterializeDictionary(IEnumerable pairs, Type[] keyAndValueTypes)
        => typeof(EffectValue)
            .GetMethod(nameof(ToDictionary), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(keyAndValueTypes)
            .Invoke(obj: null, [pairs])!;

    private static Dictionary<TKey, TValue> ToDictionary<TKey, TValue>(IEnumerable<KeyValuePair<TKey, TValue>> pairs) where TKey : notnull
        => pairs.ToDictionary();

    private static Type? GenericInterface(Type type, Type genericInterface)
        => type.GetInterfaces().FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == genericInterface);
}
