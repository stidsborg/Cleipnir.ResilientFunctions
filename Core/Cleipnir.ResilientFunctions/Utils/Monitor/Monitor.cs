using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Utils.Monitor;

public class Monitor : IMonitor
{
    private readonly IUnderlyingRegister _register;

    public Monitor(IUnderlyingRegister underlyingRegister)
    {
        _register = underlyingRegister;
    }

    public async Task<IMonitor.ILock?> Acquire(string group, string name, string lockId)
    {
        var success = await _register.CompareAndSwap(
            RegisterType.Monitor,
            group,
            name,
            expectedValue: lockId,
            newValue: lockId,
            setIfEmpty: true
        );
        if (!success) return null;

        return new Lock(this, group, name, lockId);
    }

    public async Task Release(string group, string name, string lockId) 
        => await _register.Delete(RegisterType.Monitor, group, name, expectedValue: lockId);

    private class Lock : IMonitor.ILock
    {
        private readonly IMonitor _monitor;
        private readonly string _group;
        private readonly string _instance;
        private readonly string _lockId;

        public Lock(IMonitor monitor, string group, string instance, string lockId)
        {
            _monitor = monitor;
            _group = group;
            _instance = instance;
            _lockId = lockId;
        }

        public async ValueTask DisposeAsync() => await _monitor.Release(_group, _instance, _lockId);
    }
}