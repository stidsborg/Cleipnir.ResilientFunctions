using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

public class SyncedFlag
{
    private readonly Lock _sync = new();
    private FlagPosition _position;
    private List<TaskCompletionSource> _waiters = new();
    
    public FlagPosition Position {
        get
        {
            lock (_sync)
                return _position;
        }
    }

    public bool IsRaised
    {
        get
        {
            lock (_sync)
                return _position == FlagPosition.Raised;
        }
    }

    public Task WaitForRaised() => WaitForRaised(10_000);
    
    public Task WaitForRaised(int maxWaitMs)
    {
        var tcs = new TaskCompletionSource();
        Task.Delay(maxWaitMs).ContinueWith(_ => tcs.TrySetException(new TimeoutException()));
        
        lock (_sync)
            if (_position == FlagPosition.Raised)
                return Task.CompletedTask;
            else
                _waiters.Add(tcs);

        return tcs.Task;
    }
    
    public void Raise()
    {
        List<TaskCompletionSource> waiters;
        lock (_sync)
        {
            _position = FlagPosition.Raised;
            waiters = _waiters;
            _waiters = new List<TaskCompletionSource>();
        }

        foreach (var waiter in waiters)
            waiter.SetResult();
    }
}

public enum FlagPosition
{
    Lowered, Raised
}