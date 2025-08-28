using System.Collections.Generic;

namespace Cleipnir.ResilientFunctions.Helpers;

public static class StringExtensions
{
    public static string JoinStrings(this IEnumerable<string> strings, string separator)
        => string.Join(separator, strings);
}