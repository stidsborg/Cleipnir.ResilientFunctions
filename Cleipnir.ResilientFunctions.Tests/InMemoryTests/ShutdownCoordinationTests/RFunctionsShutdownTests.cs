using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.ShutdownCoordinationTests;

[TestClass]
public class RFunctionsShutdownTests
{
    [TestMethod]
    public async Task RFunctionsShutdownTaskOnlyCompletesAfterInvokedRFuncCompletes()
    {
        var functionTypeId = "functionTypeId".ToFunctionTypeId();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(
            new InMemoryFunctionStore(),
            unhandledExceptionCatcher.Catch,
            crashedCheckFrequency: TimeSpan.FromMilliseconds(5),
            postponedCheckFrequency: TimeSpan.FromMilliseconds(5)
        );

        var insideRFuncFlag = new SyncedFlag();
        var completeRFuncFlag = new SyncedFlag();

        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            async (string _) =>
            {
                insideRFuncFlag.Raise();
                await completeRFuncFlag.WaitForRaised();
            }
        ).Invoke;

        var rFuncTask1 = rAction("1", "1");
        var rFuncTask2 = rAction("2", "2");
        
        await insideRFuncFlag.WaitForRaised();

        var shutdownTask = rFunctions.ShutdownGracefully();
        await Task.Delay(10);
        shutdownTask.IsCompleted.ShouldBeFalse();
        
        completeRFuncFlag.Raise();
        await BusyWait.UntilAsync(() => shutdownTask.IsCompleted);
        rFuncTask1.IsCompletedSuccessfully.ShouldBeTrue();
        rFuncTask2.IsCompletedSuccessfully.ShouldBeTrue();
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    [TestMethod]
    public async Task RFunctionsShutdownTaskThrowsExceptionWhenWaitThresholdIsExceeded()
    {
        var functionTypeId = "functionTypeId".ToFunctionTypeId();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(
            new InMemoryFunctionStore(),
            unhandledExceptionCatcher.Catch,
            crashedCheckFrequency: TimeSpan.FromMilliseconds(5),
            postponedCheckFrequency: TimeSpan.FromMilliseconds(5)
        );

        var insideRFuncFlag = new SyncedFlag();

        var rFunc = rFunctions.RegisterFunc(
            functionTypeId,
             (string _) =>
            {
                insideRFuncFlag.Raise();
                return NeverCompletingTask.OfType<Result>();
            }
        ).Invoke;

        _ = rFunc("1", "1");

        await insideRFuncFlag.WaitForRaised();

        var shutdownTask = rFunctions.ShutdownGracefully(TimeSpan.FromMilliseconds(100));
        await Should.ThrowAsync<TimeoutException>(shutdownTask);
    }
    
    [TestMethod]
    public async Task RFunctionsShutdownTaskOnlyCompletesAfterCrashedWatchDogsRFunctionHasCompleted()
    {
        var store = new InMemoryFunctionStore();
        var functionId = new FunctionId("someFunctionType", "someFunctionInstanceId");

        await store.CreateFunction(
            functionId,
            new StoredParameter("".ToJson(), typeof(string).SimpleQualifiedName()),
            scrapbookType: null,
            Status.Executing,
            initialEpoch: 0,
            initialSignOfLife: 0
        ).ShouldBeTrueAsync();
        
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(
            store,
            unhandledExceptionCatcher.Catch,
            crashedCheckFrequency: TimeSpan.FromMilliseconds(2),
            postponedCheckFrequency: TimeSpan.FromMilliseconds(2)
        );

        var insideRFuncFlag = new SyncedFlag();
        var completeRFuncFlag = new SyncedFlag();

        rFunctions.RegisterAction(
            functionId.TypeId,
            async (string _) =>
            {
                insideRFuncFlag.Raise();
                await completeRFuncFlag.WaitForRaised();
            }
        );

        await insideRFuncFlag.WaitForRaised();

        var shutdownTask = rFunctions.ShutdownGracefully();
        await Task.Delay(10);
        shutdownTask.IsCompleted.ShouldBeFalse();
        
        completeRFuncFlag.Raise();
        await BusyWait.UntilAsync(() => shutdownTask.IsCompleted);
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    [TestMethod]
    public async Task RFunctionsShutdownTaskOnlyCompletesAfterPostponedWatchDogsRFunctionHasCompleted()
    {
        var store = new InMemoryFunctionStore();
        var functionId = new FunctionId("someFunctionType", "someFunctionInstanceId");

        await store.CreateFunction(
            functionId,
            new StoredParameter("".ToJson(), typeof(string).SimpleQualifiedName()),
            scrapbookType: null,
            Status.Executing,
            initialEpoch: 0,
            initialSignOfLife: 0
        ).ShouldBeTrueAsync();
        
        await store.SetFunctionState(
            functionId,
            Status.Postponed,
            scrapbookJson: null,
            result: null,
            errorJson: null,
            postponedUntil: DateTime.UtcNow.AddDays(-1).Ticks,
            expectedEpoch: 0
        ).ShouldBeTrueAsync();
        
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(
            store,
            unhandledExceptionCatcher.Catch,
            crashedCheckFrequency: TimeSpan.FromMilliseconds(2),
            postponedCheckFrequency: TimeSpan.FromMilliseconds(2)
        );

        var insideRFuncFlag = new SyncedFlag();
        var completeRFuncFlag = new SyncedFlag();

        rFunctions.RegisterAction(
            functionId.TypeId,
            async (string _) =>
            {
                insideRFuncFlag.Raise();
                await completeRFuncFlag.WaitForRaised();
                return Succeed.WithoutValue;
            }
        );

        await insideRFuncFlag.WaitForRaised();

        var shutdownTask = rFunctions.ShutdownGracefully();
        await Task.Delay(10);
        shutdownTask.IsCompleted.ShouldBeFalse();
        
        completeRFuncFlag.Raise();
        await BusyWait.UntilAsync(() => shutdownTask.IsCompleted);
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
}