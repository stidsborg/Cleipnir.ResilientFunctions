using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Helpers.Disposables;

namespace Cleipnir.ResilientFunctions.ShutdownCoordination;

internal class ShutdownCoordinator
{
    private volatile bool _shutDownInitiated;
    private readonly TaskCompletionSource _shutDownCompleted = new();
    public bool ShutdownInitiated => _shutDownInitiated;
    
    private int _nextObserverId;

    private long _executingRFuncs;

    private Dictionary<int, Func<Task>> _observers = new();

    private readonly object _sync = new();

    public async Task PerformShutdown()
    {
        Dictionary<int, Func<Task>> observers;
        bool alreadyShuttingDown;
        lock (_sync)
        {
            alreadyShuttingDown = _shutDownInitiated;
            _shutDownInitiated = true;
            observers = _observers;
            _observers = new Dictionary<int, Func<Task>>();
        }

        if (alreadyShuttingDown)
        {
            await _shutDownCompleted.Task;
            return;
        }
        
        var shutdownFunctions = observers.Values.Select(_ => _).ToList();
        var shutdownTasks = new Task[shutdownFunctions.Count];
        for (var i = 0; i < shutdownFunctions.Count; i++)
        {
            var shutdownFunction = shutdownFunctions[i];
            var shutdownTask = shutdownFunction();
            shutdownTasks[i] = shutdownTask;
        }

        await Task.WhenAll(shutdownTasks);
        var executingRFuncs = Interlocked.Read(ref _executingRFuncs);
        if (executingRFuncs > 0)
            await BusyWait.ForeverUntilAsync(
                () => Interlocked.Read(ref _executingRFuncs) == 0
            );

        _shutDownCompleted.SetResult();
    }

    public void RegisterRunningRFunc()
    {
        Interlocked.Increment(ref _executingRFuncs); 
        if (!_shutDownInitiated) return;

        Interlocked.Decrement(ref _executingRFuncs);
        throw new ObjectDisposedException($"{nameof(RFunctions)} has been disposed"); 
    }
    public void RegisterRFuncCompletion() => Interlocked.Decrement(ref _executingRFuncs);

    public IDisposable RegisterRunningRFuncDisposable()
    {
        RegisterRunningRFunc();
        return new ActionDisposable(RegisterRFuncCompletion);
    }

    public bool ObserveShutdown(Func<Task> onShutdown)
    {
        lock (_sync)
        {
            if (_shutDownInitiated) return false;
            
            var observerId = ++_nextObserverId;
            _observers[observerId] = onShutdown;

            return true;
        }
    }
}