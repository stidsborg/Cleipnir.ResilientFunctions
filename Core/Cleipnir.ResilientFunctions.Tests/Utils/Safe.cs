﻿using System;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

public static class Safe
{
    public static async Task Try(Func<Task> f, Action<Exception>? onException = null)
    {
        try
        {
            await f();
        } catch (Exception e)
        {
            onException?.Invoke(e);
        }
    }
}