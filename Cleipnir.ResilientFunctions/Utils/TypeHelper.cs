using System;
using System.Text;

namespace Cleipnir.ResilientFunctions.Utils
{
    public static class TypeHelper
    {
        public static string SimpleQualifiedName(this Type type)
        {
            var assemblyQualifiedName = type.AssemblyQualifiedName;
            var builder = new StringBuilder(assemblyQualifiedName!.Length);
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
                    if (name[i + 1] != '[')
                        commas++;
                    ignore = commas > 1;
                }
                if (ignore) continue;

                stringBuilder.Append(letter);
            }

            return i;
        }
    }
}