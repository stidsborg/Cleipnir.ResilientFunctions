using System;

namespace Cleipnir.ResilientFunctions.Storage;

internal static class Helpers
{
    public static int ToInt(this string value) => int.Parse(value);
    public static long ToLong(this string value) => long.Parse(value);

    public static TEnum ToEnum<TEnum>(this int value) where TEnum : struct, Enum
        => (TEnum)(object) value;
}