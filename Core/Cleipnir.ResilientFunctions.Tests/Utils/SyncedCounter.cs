﻿using System.Threading;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

public class SyncedCounter
{
    public int Current
    {
        get
        {
            lock (_sync)
                return _current;
        }
    }

    private int _current;
    private readonly Lock _sync = new();

    public int Increment()
    {
        lock (_sync)
            return _current++;
    }

    public static SyncedCounter operator ++(SyncedCounter counter)
    {
        counter.Increment();
        return counter;
    }
}