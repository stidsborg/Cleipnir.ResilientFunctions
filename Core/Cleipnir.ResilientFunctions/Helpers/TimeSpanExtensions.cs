﻿using System;

namespace Cleipnir.ResilientFunctions.Helpers;

public static class TimeSpanExtensions
{
    public static TimeSpan RoundUpToZero(this TimeSpan t) => t < TimeSpan.Zero ? TimeSpan.Zero : t;
}