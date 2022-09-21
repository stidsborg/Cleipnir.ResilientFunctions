using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.ShutdownCoordinationTests;

[TestClass]
public class ShutdownCoordinatorTests
{
    [TestMethod]
    public async Task ShutdownTaskIsOnlyCompletedWhenRunningFunctionsReachesZero()
    {
        var shutdownCoordinator = new ShutdownCoordinator();
        var runningFuncDisposable = shutdownCoordinator.RegisterRunningRFunc();

        var shutdownCompleted = shutdownCoordinator.PerformShutdown();

        await Task.Delay(100);
        shutdownCompleted.IsCompleted.ShouldBeFalse();
        runningFuncDisposable.Dispose();
        shutdownCoordinator.ShutdownInitiated.ShouldBeTrue();
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
    public void RegisteringRunningRFuncOnDisposedShutdownCoordinatorThrowsException()
    {
        var shutdownCoordinator = new ShutdownCoordinator();
        _ = shutdownCoordinator.PerformShutdown();
        
        Should.Throw<ObjectDisposedException>(shutdownCoordinator.RegisterRunningRFunc);
    }
    
    [TestMethod]
    public void RegisteringRunningRFuncOnDisposedShutdownCoordinatorWithPositiveConfirmedIsAllowed()
    {
        var shutdownCoordinator = new ShutdownCoordinator();
        shutdownCoordinator.RegisterRunningRFunc();
        var shutdownTask = shutdownCoordinator.PerformShutdown();

        shutdownCoordinator.RegisterRunningRFunc();
        shutdownTask.IsCompleted.ShouldBeFalse();
    }
}