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
        private volatile bool _disposed;

        public SignOfLifeUpdater(
            FunctionId functionId, long expectedSignOfLife, 
            IFunctionStore functionStore, 
            TimeSpan? updateFrequency = null)
        {
            _functionId = functionId;
            _expectedSignOfLife = expectedSignOfLife;
            _functionStore = functionStore;
            _updateFrequency = updateFrequency ?? TimeSpan.FromSeconds(1);
        }

        public async Task Start()
        {
            if (_updateFrequency == TimeSpan.Zero)
                return;

            await Task.Yield();
            
            var signOfLife = _expectedSignOfLife;
            
            while (!_disposed)
            {
                await Task.Delay(_updateFrequency);

                var success = await _functionStore.UpdateSignOfLife(
                    _functionId,  
                    signOfLife, 
                    signOfLife = DateTime.UtcNow.Ticks
                );
                    
                if (!success) return;
            }
        }

        public void Dispose() => _disposed = true;
    }
}