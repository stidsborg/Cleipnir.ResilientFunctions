using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public class SignOfLifeUpdater : IDisposable
{
    private readonly FunctionId _functionId;
    private readonly int _epoch;

    private readonly TimeSpan _signFrequency;
    
    private readonly IFunctionStore _functionStore;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private volatile bool _disposed;

    public SignOfLifeUpdater(
        FunctionId functionId, 
        int epoch, 
        IFunctionStore functionStore,
        UnhandledExceptionHandler unhandledExceptionHandler,
        TimeSpan signFrequency)
    {
        _functionId = functionId;
        _epoch = epoch;
            
        _functionStore = functionStore;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _signFrequency = signFrequency;
    }

    public static IDisposable CreateAndStart(
        FunctionId functionId, 
        int epoch, 
        IFunctionStore functionStore, 
        SettingsWithDefaults settings)
    {
        var signOfLifeUpdater = new SignOfLifeUpdater(
            functionId,
            epoch,
            functionStore,
            settings.UnhandledExceptionHandler,
            signFrequency: settings.SignOfLifeFrequency
        );
            
        _ = signOfLifeUpdater.Start();
        return signOfLifeUpdater;
    }
    
    public Task Start()
    {
        if (_signFrequency == TimeSpan.Zero)
        {
            _disposed = true;
            return Task.CompletedTask;
        }

        return Task.Run(async () =>
        {
            while (!_disposed)
            {
                try
                {
                    await Task.Delay(_signFrequency);

                    if (_disposed) return;

                    var success = await _functionStore.RenewLease(
                        _functionId,
                        expectedEpoch: _epoch,
                        leaseExpiration: DateTime.UtcNow.Ticks + (_signFrequency * 2).Ticks
                    );

                    _disposed = !success;
                }
                catch (Exception e)
                {
                    _disposed = true;
                    _unhandledExceptionHandler.Invoke(
                        new FrameworkException(
                            _functionId.TypeId,
                            $"{nameof(SignOfLifeUpdater)} failed while executing: '{_functionId}'",
                            e
                        )
                    );
                }
            }
        });
    }

    public void Dispose() => _disposed = true;
}