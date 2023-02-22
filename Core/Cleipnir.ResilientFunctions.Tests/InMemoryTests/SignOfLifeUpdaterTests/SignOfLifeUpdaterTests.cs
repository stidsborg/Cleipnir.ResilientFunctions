using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.SignOfLifeUpdaterTests;

[TestClass]
public class SignOfLifeUpdaterTests
{
    private readonly FunctionId _functionId = new FunctionId("functionId", "instanceId");
    private UnhandledExceptionCatcher _unhandledExceptionCatcher = new();
        
    [TestInitialize]
    public void SetUp() => _unhandledExceptionCatcher = new UnhandledExceptionCatcher();

    [TestMethod]
    public async Task AfterSignOfLifeIsStartedStoreIsInvokedContinuouslyWithExpectedDelay()
    {
        const int expectedEpoch = 100;
        var invocations = new SyncedList<Parameters>();
        var store = new SignOfLifeTestFunctionStore(
            (id, epoch, life, _) =>
            {
                invocations.Add(new Parameters(id, ExpectedEpoch: epoch, NewSignOfLife: life));
                return true;
            });
        
        var settings = new Settings(
            _unhandledExceptionCatcher.Catch,
            crashedCheckFrequency: TimeSpan.FromMilliseconds(10)
        );
        var updater = SignOfLifeUpdater.CreateAndStart(
            _functionId,
            expectedEpoch,
            store,
            SettingsWithDefaults.Default.Merge(settings)
        );

        await Task.Delay(200);
        updater.Dispose();

        invocations.Count.ShouldBeGreaterThan(2);
        invocations.All(p => p.FunctionId == _functionId).ShouldBeTrue();

        const int expectedInitialSignOfLife = 0;
        _ = invocations.Aggregate(expectedInitialSignOfLife, (prevSignOfLife, parameters) =>
        {
            parameters.ExpectedEpoch.ShouldBe(expectedEpoch);
            prevSignOfLife.ShouldBeLessThan(parameters.NewSignOfLife);
            return parameters.NewSignOfLife;
        });
    }

    [TestMethod]
    public async Task SignOfLifeStopsInvokingStoreWhenFalseIsReturnedFromStore()
    {
        var syncedCounter = new SyncedCounter();
        var store = new SignOfLifeTestFunctionStore((id, epoch, life, _) =>
        {
            syncedCounter.Increment();
            return false;
        });

        var settings = new Settings(
            _unhandledExceptionCatcher.Catch,
            crashedCheckFrequency: TimeSpan.FromMilliseconds(10)
        );
        var updater = SignOfLifeUpdater.CreateAndStart(
            _functionId,
            epoch: 0,
            store,
            SettingsWithDefaults.Default.Merge(settings)
        );

        await Task.Delay(100);
        updater.Dispose();

        syncedCounter.Current.ShouldBe(1);
        _unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
        
    [TestMethod]
    public void WhenFunctionStoreThrowsExceptionAnTheUnhandledExceptionActionIsInvokedWithAFrameworkException()
    {
        var syncedCounter = new SyncedCounter();
        var store = new SignOfLifeTestFunctionStore(
            (id, epoch, life, _) =>
            {
                syncedCounter.Increment();
                throw new Exception();
            });

        var settings = new Settings(
            _unhandledExceptionCatcher.Catch,
            crashedCheckFrequency: TimeSpan.FromMilliseconds(10)
        );
        var updater = SignOfLifeUpdater.CreateAndStart(
            _functionId,
            epoch: 0,
            store,
            SettingsWithDefaults.Default.Merge(settings)
        );

        BusyWait.Until(() => _unhandledExceptionCatcher.ThrownExceptions.Any());
            
        _unhandledExceptionCatcher.ThrownExceptions.Count.ShouldBe(1);
        var thrownException = _unhandledExceptionCatcher.ThrownExceptions[0];
        (thrownException is FrameworkException).ShouldBeTrue();
        syncedCounter.Current.ShouldBe(1);
    }

    private record Parameters(FunctionId FunctionId, int ExpectedEpoch, int NewSignOfLife);
}