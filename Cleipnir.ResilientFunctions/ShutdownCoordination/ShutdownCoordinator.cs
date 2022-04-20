using System;
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

    public IDisposable RegisterRunningRFunc()
    {
        Interlocked.Increment(ref _executingRFuncs); 
        if (!_shutDownInitiated) 
            return new ActionDisposable(() => Interlocked.Decrement(ref _executingRFuncs));;

        Interlocked.Decrement(ref _executingRFuncs);
        throw new ObjectDisposedException($"{nameof(RFunctions)} has been disposed"); 
        
       
    }
}