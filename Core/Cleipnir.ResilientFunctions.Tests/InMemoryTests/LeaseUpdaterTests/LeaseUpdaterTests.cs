using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.LeaseUpdaterTests;

[TestClass]
public class LeaseUpdaterTests
{
    private readonly FlowId _flowId = new FlowId("functionId", "instanceId");
    private readonly StoredId _storedId = new StoredId(0.ToStoredType(), "instanceId".ToStoredInstance());
    
    private UnhandledExceptionCatcher _unhandledExceptionCatcher = new();
        
    [TestInitialize]
    public void SetUp() => _unhandledExceptionCatcher = new UnhandledExceptionCatcher();

    [TestMethod]
    public async Task AfterLeaseUpdaterIsStartedStoreIsInvokedContinuouslyWithExpectedDelay()
    {
        const int expectedEpoch = 100;
        var invocations = new SyncedList<Parameters>();
        var store = new LeaseUpdaterTestFunctionStore(
            (id, epoch, leaseExpiry) =>
            {
                invocations.Add(new Parameters(id, ExpectedEpoch: epoch, LeaseExpiry: leaseExpiry));
                return true;
            });
        
        var settings = new Settings(
            _unhandledExceptionCatcher.Catch,
            leaseLength: TimeSpan.FromMilliseconds(10)
        );
        var updater = LeaseUpdater.CreateAndStart(
            _storedId,
            _flowId,
            expectedEpoch,
            store,
            SettingsWithDefaults.Default.Merge(settings)
        );

        await Task.Delay(200);
        updater.Dispose();

        invocations.Count.ShouldBeGreaterThan(2);
        invocations.All(p => p.StoredId == _storedId).ShouldBeTrue();

        const long expectedInitialLeaseExpiry = 0;
        _ = invocations.Aggregate(expectedInitialLeaseExpiry, (prevLeaseExpiry, parameters) =>
        {
            parameters.ExpectedEpoch.ShouldBe(expectedEpoch);
            prevLeaseExpiry.ShouldBeLessThan(parameters.LeaseExpiry);
            return parameters.LeaseExpiry;
        });
    }

    [TestMethod]
    public async Task LeaseUpdaterStopsInvokingStoreWhenFalseIsReturnedFromStore()
    {
        var syncedCounter = new SyncedCounter();
        var store = new LeaseUpdaterTestFunctionStore((id, epoch, life) =>
        {
            syncedCounter.Increment();
            return false;
        });

        var settings = new Settings(
            _unhandledExceptionCatcher.Catch,
            leaseLength: TimeSpan.FromMilliseconds(10)
        );
        var updater = LeaseUpdater.CreateAndStart(
            _storedId,
            _flowId,
            epoch: 0,
            store,
            SettingsWithDefaults.Default.Merge(settings)
        );

        await Task.Delay(100);
        updater.Dispose();

        syncedCounter.Current.ShouldBe(1);
        _unhandledExceptionCatcher.ThrownExceptions.ShouldNotBeEmpty();
    }
        
    [TestMethod]
    public async Task WhenFunctionStoreThrowsExceptionAnTheUnhandledExceptionActionIsInvokedWithAFrameworkException()
    {
        var syncedCounter = new SyncedCounter();
        var store = new LeaseUpdaterTestFunctionStore(
            (id, epoch, life) =>
            {
                syncedCounter.Increment();
                throw new Exception();
            });

        var settings = new Settings(
            _unhandledExceptionCatcher.Catch,
            leaseLength: TimeSpan.FromMilliseconds(10)
        );
        using var updater = LeaseUpdater.CreateAndStart(
            _storedId,
            _flowId,
            epoch: 0,
            store,
            SettingsWithDefaults.Default.Merge(settings)
        );

        await BusyWait.Until(() => _unhandledExceptionCatcher.ThrownExceptions.Any());
            
        _unhandledExceptionCatcher.ThrownExceptions.Count.ShouldBe(1);
        var thrownException = _unhandledExceptionCatcher.ThrownExceptions[0];
        (thrownException is FrameworkException).ShouldBeTrue();
        syncedCounter.Current.ShouldBe(1);
    }

    private record Parameters(StoredId StoredId, int ExpectedEpoch, long LeaseExpiry);
}