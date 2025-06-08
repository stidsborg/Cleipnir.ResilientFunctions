using System.Collections.Generic;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

public static class EnumerableLinq
{
    public static IEnumerable<T> AsEnumerable<T>(this T t) => [t];
    public static string JoinStrings(this IEnumerable<string> strings, string separator) => string.Join(separator, strings);
}