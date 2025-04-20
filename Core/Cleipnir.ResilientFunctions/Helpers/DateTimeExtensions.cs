using System;

namespace Cleipnir.ResilientFunctions.Helpers;

internal static class DateTimeExtensions
{
    public static DateTime ToDateTime(this long ticks) => new(ticks, DateTimeKind.Utc);
}