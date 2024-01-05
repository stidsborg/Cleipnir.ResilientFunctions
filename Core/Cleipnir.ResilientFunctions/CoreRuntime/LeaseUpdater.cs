using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

internal class LeaseUpdater : IDisposable
{
    private readonly FunctionId _functionId;
    private readonly int _epoch;

    private readonly TimeSpan _leaseLength;
    
    private readonly IFunctionStore _functionStore;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private volatile bool _disposed;

    private LeaseUpdater(
        FunctionId functionId, 
        int epoch, 
        IFunctionStore functionStore,
        UnhandledExceptionHandler unhandledExceptionHandler,
        TimeSpan leaseLength)
    {
        _functionId = functionId;
        _epoch = epoch;
            
        _functionStore = functionStore;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _leaseLength = leaseLength;
    }

    public static IDisposable CreateAndStart(FunctionId functionId, int epoch, IFunctionStore functionStore, SettingsWithDefaults settings)
    {
        var leaseUpdater = new LeaseUpdater(
            functionId,
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
        if (_leaseLength == TimeSpan.Zero)
        {
            _disposed = true;
            return;
        }
        
        while (!_disposed)
        {
            try
            {
                await Task.Delay(_leaseLength / 2);

                if (_disposed) return;

                var success = await _functionStore.RenewLease(
                    _functionId,
                    expectedEpoch: _epoch,
                    leaseExpiration: DateTime.UtcNow.Ticks + _leaseLength.Ticks
                );

                _disposed = !success;
            }
            catch (Exception e)
            {
                _disposed = true;
                _unhandledExceptionHandler.Invoke(
                    new FrameworkException(
                        _functionId.TypeId,
                        $"{nameof(LeaseUpdater)} failed while executing function: '{_functionId}'",
                        e
                    )
                );
            }
        }
    }

    public void Dispose() => _disposed = true;
}