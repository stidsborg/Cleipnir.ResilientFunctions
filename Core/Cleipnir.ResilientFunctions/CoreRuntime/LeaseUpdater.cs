using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

internal class LeaseUpdater : IDisposable
{
    private readonly FlowId _flowId;
    private readonly StoredId _storedId;
    private readonly int _epoch;

    private readonly TimeSpan _leaseLength;
    
    private readonly IFunctionStore _functionStore;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private volatile bool _disposed;

    private LeaseUpdater(
        FlowId flowId,
        StoredId storedId, 
        int epoch, 
        IFunctionStore functionStore,
        UnhandledExceptionHandler unhandledExceptionHandler,
        TimeSpan leaseLength)
    {
        _flowId = flowId;
        _storedId = storedId;
        _epoch = epoch;
            
        _functionStore = functionStore;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _leaseLength = leaseLength;
    }

    public static IDisposable CreateAndStart(StoredId storedId, FlowId flowId, int epoch, IFunctionStore functionStore, SettingsWithDefaults settings)
    {
        var leaseUpdater = new LeaseUpdater(
            flowId,
            storedId,
            epoch,
            functionStore,
            settings.UnhandledExceptionHandler,
            leaseLength: settings.LeaseLength
        );

        Task.Run(leaseUpdater.Start);
        return leaseUpdater;
    }

    private async Task Start()
    {
        if (_leaseLength == TimeSpan.Zero || _leaseLength == TimeSpan.MaxValue)
            _disposed = true;
        
        while (!_disposed)
        {
            try
            {
                await Task.Delay(_leaseLength / 2);

                if (_disposed) return;

                var success = await _functionStore.RenewLease(
                    _storedId,
                    expectedEpoch: _epoch,
                    leaseExpiration: DateTime.UtcNow.Ticks + _leaseLength.Ticks
                );

                if (!success)
                {
                    _disposed = true;
                    _unhandledExceptionHandler.Invoke(UnexpectedStateException.LeaseUpdateFailed(_flowId));
                }
            }
            catch (Exception e)
            {
                _disposed = true;
                _unhandledExceptionHandler.Invoke(
                    new FrameworkException(
                        $"{nameof(LeaseUpdater)} failed for '{_storedId}'",
                        e,
                        _flowId
                    )
                );
            }
        }
    }

    public void Dispose() => _disposed = true;
}