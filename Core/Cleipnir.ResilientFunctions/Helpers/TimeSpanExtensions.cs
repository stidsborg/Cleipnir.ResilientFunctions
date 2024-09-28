using System;

namespace Cleipnir.ResilientFunctions.Helpers;

internal static class TimeSpanExtensions
{
    public static TimeSpan RoundUpToZero(this TimeSpan t) => t < TimeSpan.Zero ? TimeSpan.Zero : t;
}