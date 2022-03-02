using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.ShutdownCoordination;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.ShutdownCoordinationTests;

[TestClass]
public class ShutdownCoordinatorTests
{
    [TestMethod]
    public async Task ShutdownTaskIsOnlyCompletedAfterObserverTaskCompletes()
    {
        var shutdownCoordinator = new ShutdownCoordinator();
        var insideNotifier = new SyncedFlag();
        var completeNotifier = new SyncedFlag();

        shutdownCoordinator.ObserveShutdown(async () =>
        {
            insideNotifier.Raise();
            await completeNotifier.WaitForRaised();
        });

        var shutdownCompleted = shutdownCoordinator.PerformShutdown();
        await insideNotifier.WaitForRaised();

        await Task.Delay(10);
        shutdownCompleted.IsCompleted.ShouldBeFalse();

        completeNotifier.Raise();
        await BusyWait.UntilAsync(() => shutdownCompleted.IsCompleted);
    }
    
    [TestMethod]
    public void ShutdownTaskIsCompletedImmediatelyWhenThereAreObservers()
    {
        var shutdownCoordinator = new ShutdownCoordinator();

        var shutdownCompleted = shutdownCoordinator.PerformShutdown();
        shutdownCompleted.IsCompletedSuccessfully.ShouldBeTrue();
    }
    
    [TestMethod]
    public void ObserveShutdownReturnsFalseWhenShutdownHasAlreadyCompleted()
    {
        var shutdownCoordinator = new ShutdownCoordinator();
        shutdownCoordinator.PerformShutdown();

        var success = shutdownCoordinator.ObserveShutdown(() => Task.CompletedTask);
        success.ShouldBeFalse();
    }

    [TestMethod]
    public async Task ShutdownTaskOnlyCompletesAfterObserverTasksCompletes()
    {
        var shutdownCoordinator = new ShutdownCoordinator();
        var insideNotifier = new SyncedFlag();
        var completeNotifier = new SyncedFlag();
        
        shutdownCoordinator.ObserveShutdown(async () =>
        {
            insideNotifier.Raise();
            await completeNotifier.WaitForRaised();
        });

        shutdownCoordinator.ObserveShutdown(() => Task.CompletedTask);

        var shutdownCompleted = shutdownCoordinator.PerformShutdown();
        await insideNotifier.WaitForRaised();

        await Task.Delay(10);
        shutdownCompleted.IsCompleted.ShouldBeFalse();

        completeNotifier.Raise();
        await BusyWait.UntilAsync(() => shutdownCompleted.IsCompleted);
    }
    
    [TestMethod]
    public async Task ShutdownTaskOnlyCompletesAfterObserverTasksAndRFuncsCompletes()
    {
        var shutdownCoordinator = new ShutdownCoordinator();
        var insideNotifier = new SyncedFlag();
        var completeNotifier = new SyncedFlag();
        
        shutdownCoordinator.ObserveShutdown(async () =>
        {
            insideNotifier.Raise();
            await completeNotifier.WaitForRaised();
        });

        shutdownCoordinator.RegisterRunningRFunc();

        var shutdownCompleted = shutdownCoordinator.PerformShutdown();
        await insideNotifier.WaitForRaised();

        await Task.Delay(10);
        shutdownCompleted.IsCompleted.ShouldBeFalse();
        
        completeNotifier.Raise();

        await Task.Delay(10);
        shutdownCompleted.IsCompleted.ShouldBeFalse();

        shutdownCoordinator.RegisterRFuncCompletion();
        await BusyWait.UntilAsync(() => shutdownCompleted.IsCompleted);
    }
    
    [TestMethod]
    public void RegisteringRunningRFuncOnDisposedShutdownCoordinatorThrowsException()
    {
        var shutdownCoordinator = new ShutdownCoordinator();
        _ = shutdownCoordinator.PerformShutdown();
        
        Should.Throw<ObjectDisposedException>(shutdownCoordinator.RegisterRunningRFunc);
    }
}