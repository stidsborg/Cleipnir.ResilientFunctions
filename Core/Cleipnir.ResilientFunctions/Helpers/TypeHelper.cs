using System;
using System.Text;

namespace Cleipnir.ResilientFunctions.Helpers;

internal static class TypeHelper
{
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
    {
        var assemblyQualifiedName = type.AssemblyQualifiedName;
        if (assemblyQualifiedName == null)
            return type.FullName ?? type.Name;

        var builder = new StringBuilder(assemblyQualifiedName.Length);
        ExtractSimplifiedName(assemblyQualifiedName, 0, builder);
        return builder.ToString();
    }

    private static int ExtractSimplifiedName(string name, int i, StringBuilder stringBuilder)
    {
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