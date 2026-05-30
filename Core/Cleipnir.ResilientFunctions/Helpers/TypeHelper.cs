using System;
using System.Collections.Concurrent;
using System.Text;

namespace Cleipnir.ResilientFunctions.Helpers;

public static class TypeHelper
{
    // Type -> simplified name is a pure mapping and is computed on every serialize,
    // so the result is cached to avoid re-parsing the assembly-qualified name repeatedly.
    private static readonly ConcurrentDictionary<Type, string> SimpleQualifiedNameCache = new();

    /// <summary>
    /// Creates a simplified assembly qualified name by removing version, culture, and public key token information.
    /// This is useful for serialization scenarios where type portability across assembly versions is needed.
    /// </summary>
    /// <param name="type">The type to get the simplified qualified name for.</param>
    /// <returns>A simplified assembly qualified name (e.g., "System.String, System.Private.CoreLib" instead of full version info).</returns>
    /// <example>
    /// For a type like List&lt;string&gt;, this returns:
    /// "System.Collections.Generic.List`1[[System.String, System.Private.CoreLib]], System.Private.CoreLib"
    /// instead of the full assembly qualified name with version/culture/token.
    /// </example>
    public static string SimpleQualifiedName(this Type type)
        => SimpleQualifiedNameCache.GetOrAdd(type, static t =>
        {
            var assemblyQualifiedName = t.AssemblyQualifiedName;
            if (assemblyQualifiedName == null)
                return t.FullName ?? t.Name;

            var builder = new StringBuilder(assemblyQualifiedName.Length);
            ExtractSimplifiedName(assemblyQualifiedName, 0, builder);
            return builder.ToString();
        });

    // Walks one bracket-scope of an assembly-qualified name, copying it while dropping
    // the Version/Culture/PublicKeyToken segments. Recurses on '[' so each nested generic
    // argument (and array suffix) is processed in its own scope; returns the index of the
    // ']' that closed this scope so the caller can continue past it.
    private static int ExtractSimplifiedName(string name, int i, StringBuilder stringBuilder)
    {
        // Within a scope the assembly qualifier is "TypeName, Assembly, Version=.., Culture=.., PublicKeyToken=..".
        // The first "real" comma separates the type name from the assembly name (kept); any further commas
        // begin Version/Culture/Token (dropped). Commas of the form "],[" separate generic arguments and are
        // not assembly qualifiers, so they are excluded from the count via the lookahead below.
        var ignore = false;
        var commas = 0;
        for (; i < name.Length; i++)
        {
            var letter = name[i];
            if (letter == '[')
            {
                stringBuilder.Append('[');
                i = ExtractSimplifiedName(name, i + 1, stringBuilder);
                continue;
            }
            else if (letter == ']')
            {
                stringBuilder.Append(']');
                return i;
            }
            else if (letter == ',')
            {
                // A comma directly before '[' is a generic-argument separator ("],["), not an assembly qualifier.
                if (i + 1 < name.Length && name[i + 1] != '[')
                    commas++;
                ignore = commas > 1;
            }
            if (ignore) continue;

            stringBuilder.Append(letter);
        }

        return i;
    }
}