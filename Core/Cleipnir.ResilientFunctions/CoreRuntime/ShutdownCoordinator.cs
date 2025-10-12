using System;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Helpers.Disposables;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

internal class ShutdownCoordinator
{
    private volatile bool _shutDownInitiated;
    public bool ShutdownInitiated => _shutDownInitiated;
    private long _candidates;
    private long _confirmed;

    public async Task PerformShutdown()
    {
        _shutDownInitiated = true;
        await BusyWait.ForeverUntilAsync(
            () => Interlocked.Read(ref _candidates) == 0
        );
    }
    
    public void SignalShutdown() => _shutDownInitiated = true;

    public IDisposable RegisterRunningFunction()
    {
        var disposable = TryRegisterRunningFunction();
        if (disposable == null)
            throw new ObjectDisposedException($"{nameof(FunctionsRegistry)} has been disposed");

        return disposable;
    }
    
    public IDisposable? TryRegisterRunningFunction()
    {
        Interlocked.Increment(ref _candidates);
        if (_shutDownInitiated && Interlocked.Read(ref _confirmed) == 0)
        {
            Interlocked.Decrement(ref _candidates);
            return null;
        }

        Interlocked.Increment(ref _confirmed);
        return new ActionDisposable(() =>
        {
            Interlocked.Decrement(ref _confirmed);
            Interlocked.Decrement(ref _candidates);
        });
    }
}