using System;

namespace Cleipnir.ResilientFunctions.Helpers;

public static class TimeSpanHelper
{
    public static TimeSpan Max(TimeSpan t1, TimeSpan t2) => t1 > t2 ? t1 : t2;
}