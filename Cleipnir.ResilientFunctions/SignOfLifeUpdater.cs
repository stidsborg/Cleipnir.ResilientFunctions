using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions
{
    public class SignOfLifeUpdater : IDisposable
    {
        private readonly FunctionId _functionId;
        private readonly long _expectedSignOfLife;

        private readonly TimeSpan _updateFrequency; 
        
        private readonly IFunctionStore _functionStore;
        private readonly Action<RFunctionException> _unhandledExceptionHandler;
        private volatile bool _disposed;

        public SignOfLifeUpdater(
            FunctionId functionId, long expectedSignOfLife, 
            IFunctionStore functionStore,
            Action<RFunctionException> unhandledExceptionHandler,
            TimeSpan? updateFrequency = null)
        {
            _functionId = functionId;
            _expectedSignOfLife = expectedSignOfLife;
            _functionStore = functionStore;
            _unhandledExceptionHandler = unhandledExceptionHandler;
            _updateFrequency = updateFrequency ?? TimeSpan.FromSeconds(1);
        }

        public Task Start()
        {
            if (_updateFrequency == TimeSpan.Zero) return Task.CompletedTask;

            return Task.Run(async () =>
            {
                var signOfLife = _expectedSignOfLife;

                try
                {
                    while (!_disposed)
                    {
                        await Task.Delay(_updateFrequency);

                        if (_disposed) return;

                        var success = await _functionStore.UpdateSignOfLife(
                            _functionId, 
                            signOfLife,
                            signOfLife = DateTime.UtcNow.Ticks
                        );

                        if (!success) return;
                    }
                }
                catch (Exception e)
                {
                    _unhandledExceptionHandler( 
                        new FrameworkException(
                            $"SignOfLifeUpdater failed while executing: '{_functionId}'", 
                            e
                        )
                    );
                }
            });
        }

        public void Dispose() => _disposed = true;
    }
}