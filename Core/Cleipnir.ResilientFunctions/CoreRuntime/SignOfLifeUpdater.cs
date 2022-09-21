using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.SignOfLife;

public class SignOfLifeUpdater : IDisposable
{
    private readonly FunctionId _functionId;
    private readonly int _leader;

    private readonly TimeSpan _updateFrequency; 
        
    private readonly IFunctionStore _functionStore;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private volatile bool _disposed;

    public SignOfLifeUpdater(
        FunctionId functionId, 
        int leader, 
        IFunctionStore functionStore,
        UnhandledExceptionHandler unhandledExceptionHandler,
        TimeSpan updateFrequency)
    {
        _functionId = functionId;
        _leader = leader;
            
        _functionStore = functionStore;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _updateFrequency = updateFrequency;
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
            updateFrequency: settings.CrashedCheckFrequency / 2
        );
            
        _ = signOfLifeUpdater.Start();
        return signOfLifeUpdater;
    }
    
    public Task Start()
    {
        if (_updateFrequency == TimeSpan.Zero)
        {
            _disposed = true;
            return Task.CompletedTask;
        }

        return Task.Run(async () =>
        {
            var heartBeat = 1;
            while (!_disposed)
            {
                try
                {
                    await Task.Delay(_updateFrequency);

                    if (_disposed) return;

                    var success = await _functionStore.UpdateSignOfLife(
                        _functionId,
                        _leader,
                        heartBeat++
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