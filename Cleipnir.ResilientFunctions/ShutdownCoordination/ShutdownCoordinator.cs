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
    public bool ShutdownInitiated => _shutDownInitiated;
    private long _executingRFuncs;

    public async Task PerformShutdown()
    {
        _shutDownInitiated = true;
        await BusyWait.ForeverUntilAsync(
            () => Interlocked.Read(ref _executingRFuncs) == 0
        );
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
}