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
    private long _candidates;
    private long _confirmed;
    private readonly object _sync = new();

    public async Task PerformShutdown()
    {
        _shutDownInitiated = true;
        await BusyWait.ForeverUntilAsync(
            () => Interlocked.Read(ref _candidates) == 0
        );
    }

    public IDisposable RegisterRunningRFunc()
    {
        Interlocked.Increment(ref _candidates);
        if (_shutDownInitiated && Interlocked.Read(ref _confirmed) == 0)
        {
            Interlocked.Decrement(ref _candidates);
            throw new ObjectDisposedException($"{nameof(RFunctions)} has been disposed");
        }

        Interlocked.Increment(ref _confirmed);
        return new ActionDisposable(() =>
        {
            Interlocked.Decrement(ref _confirmed);
            Interlocked.Decrement(ref _candidates);
        });
    }
}