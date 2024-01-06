using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Cleipnir.ResilientFunctions.Utils;
using Cleipnir.ResilientFunctions.Utils.Monitor;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.UtilsTests;

[TestClass]
public class MonitorUnitTests
{
    [TestMethod]
    public async Task MultipleLocksCanBeAcquiredInSortedOrderOnName()
    {
        var callbacks = new List<Tuple<string, string, string, bool>>();
        
        IMonitor monitor = new Monitor(new DecoratedRegister(
            compareAndSwapCallback: (group, name, lockId, success) => callbacks.Add(Tuple.Create(group, name, lockId, success)))
        );

        var acquiredLocks = await monitor.Acquire(
            new LockInfo("SomeGroup", Name: "B", "someLockId"),
            new LockInfo("SomeGroup", Name: "A", "someLockId")
        );
        
        callbacks.Count.ShouldBe(2);
        callbacks[0].Item2.ShouldBe("A");
        callbacks[1].Item2.ShouldBe("B");
        callbacks.All(t => t.Item4).ShouldBeTrue();

        acquiredLocks.ShouldNotBeNull();
        await acquiredLocks.DisposeAsync();

        //check locks are free after dispose
        await monitor.Acquire("SomeGroup", "B", "someOtherLockId").ShouldNotBeNullAsync();
        await monitor.Acquire("SomeGroup", "A", "someOtherLockId").ShouldNotBeNullAsync();
    }
    
    [TestMethod]
    public async Task MultipleLocksCanBeAcquiredInSortedOrderOnGroup()
    {
        var callbacks = new List<Tuple<string, string, string, bool>>();
        
        IMonitor monitor = new Monitor(new DecoratedRegister(
            compareAndSwapCallback: (group, name, lockId, success) => callbacks.Add(Tuple.Create(group, name, lockId, success)))
        );
        
        var acquiredLocks = await monitor.Acquire(
            new LockInfo(GroupId: "B", "SomeName", "someLockId"),
            new LockInfo(GroupId: "A", "SomeName", "someLockId")
        );
        
        callbacks.Count.ShouldBe(2);
        callbacks[0].Item1.ShouldBe("A");
        callbacks[1].Item1.ShouldBe("B");
        callbacks.All(t => t.Item4).ShouldBeTrue();
        
        acquiredLocks.ShouldNotBeNull();
        await acquiredLocks.DisposeAsync();

        //check locks are free after dispose
        await monitor.Acquire("B", "SomeName", "someOtherLockId").ShouldNotBeNullAsync();
        await monitor.Acquire("A", "SomeName", "someOtherLockId").ShouldNotBeNullAsync();
    }
    
    [TestMethod]
    public async Task PreviousAcquiredLockIsReleasedWhenLaterLockCannotBeAcquired()
    {
        IMonitor monitor = new Monitor(new DecoratedRegister(compareAndSwapCallback: (_, _, _, _) => {}));

        await monitor.Acquire("A", "SomeName", "someOtherLockId");
        
        await monitor.Acquire(
            new LockInfo(GroupId: "B", "SomeName", "someLockId"),
            new LockInfo(GroupId: "A", "SomeName", "someLockId")
        );

        await monitor.Acquire("B", "SomeName", "someOtherLockId").ShouldNotBeNullAsync();
    }

    private class DecoratedRegister : IUnderlyingRegister
    {
        private readonly Action<string, string, string, bool> _compareAndSwapCallback;
        private readonly IUnderlyingRegister _inner = new UnderlyingInMemoryRegister();

        public DecoratedRegister(Action<string, string, string, bool> compareAndSwapCallback) 
            => _compareAndSwapCallback = compareAndSwapCallback;
        
        public Task<bool> SetIfEmpty(RegisterType registerType, string group, string name, string value)
        {
            return _inner.SetIfEmpty(registerType, group, name, value);
        }

        public async Task<bool> CompareAndSwap(RegisterType registerType, string group, string name, string newValue, string expectedValue, bool setIfEmpty = true)
        {
            var success = await _inner.CompareAndSwap(registerType, group, name, newValue, expectedValue, setIfEmpty);
            _compareAndSwapCallback(group, name, newValue, success);
            
            return success;
        }

        public Task<string?> Get(RegisterType registerType, string group, string name)
        {
            return _inner.Get(registerType, group, name);
        }

        public Task<bool> Delete(RegisterType registerType, string group, string name, string expectedValue)
        {
            return _inner.Delete(registerType, group, name, expectedValue);
        }

        public Task Delete(RegisterType registerType, string group, string name)
        {
            return _inner.Delete(registerType, group, name);
        }

        public Task<bool> Exists(RegisterType registerType, string group, string name)
        {
            return _inner.Exists(registerType, group, name);
        }
    }
}