using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage.Utils;

public static class Escaper
{
    public static char Separator => Delimiters.UnitSeparator;
    public static string Escape(params string[] strings) => string.Join(Separator, strings);
    public static string[] Unescape(string escapedString) => escapedString.Split(Separator);
}