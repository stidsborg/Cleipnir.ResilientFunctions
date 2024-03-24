using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage.Utils;

public static class Escaper
{
    public static string Escape(params string[] strings) => string.Join(Delimiters.UnitSeparator, strings);
    public static string[] Unescape(string escapedString) => escapedString.Split(Delimiters.UnitSeparator);
}