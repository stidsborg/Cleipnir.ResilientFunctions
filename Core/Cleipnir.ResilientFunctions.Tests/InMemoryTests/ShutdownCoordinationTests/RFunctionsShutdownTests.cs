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
        var flowType = "flowType".ToFlowType();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            new InMemoryFunctionStore(),
            new Settings(
                unhandledExceptionCatcher.Catch,
                leaseLength: TimeSpan.FromMilliseconds(10),
                watchdogCheckFrequency: TimeSpan.FromMilliseconds(10)
            )
        );

        var insideRFuncFlag = new SyncedFlag();
        var completeRFuncFlag = new SyncedFlag();

        var rAction = functionsRegistry.RegisterAction(
            flowType,
            async (string _) =>
            {
                insideRFuncFlag.Raise();
                await completeRFuncFlag.WaitForRaised();
            }
        ).Invoke;

        var rFuncTask1 = rAction("1", "1");
        var rFuncTask2 = rAction("2", "2");
        
        await insideRFuncFlag.WaitForRaised();

        var shutdownTask = functionsRegistry.ShutdownGracefully();
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
        var flowType = "flowType".ToFlowType();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            new InMemoryFunctionStore(),
            new Settings(
                unhandledExceptionCatcher.Catch,
                leaseLength: TimeSpan.FromMilliseconds(10),
                watchdogCheckFrequency: TimeSpan.FromMilliseconds(10)
            )
        );

        var insideRFuncFlag = new SyncedFlag();

        var rFunc = functionsRegistry.RegisterFunc(
            flowType,
             (string _) =>
            {
                insideRFuncFlag.Raise();
                return NeverCompletingTask.OfType<Result>();
            }
        ).Invoke;

        _ = rFunc("1", "1");

        await insideRFuncFlag.WaitForRaised();

        var shutdownTask = functionsRegistry.ShutdownGracefully(TimeSpan.FromMilliseconds(100));
        await Should.ThrowAsync<TimeoutException>(shutdownTask);
    }
    
    [TestMethod]
    public async Task RFunctionsShutdownTaskOnlyCompletesAfterCrashedWatchDogsRFunctionHasCompleted()
    {
        var store = new InMemoryFunctionStore();
        var functionId = new FlowId("someFunctionType", "someflowInstance");

        await store.CreateFunction(
            functionId,
            param: "".ToJson(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();
        
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                leaseLength: TimeSpan.FromMilliseconds(10),
                watchdogCheckFrequency: TimeSpan.FromMilliseconds(10)
            )
        );

        var insideRFuncFlag = new SyncedFlag();
        var completeRFuncFlag = new SyncedFlag();

        functionsRegistry.RegisterAction(
            functionId.Type,
            async (string _) =>
            {
                insideRFuncFlag.Raise();
                await completeRFuncFlag.WaitForRaised();
            }
        );

        await insideRFuncFlag.WaitForRaised();

        var shutdownTask = functionsRegistry.ShutdownGracefully();
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
        var functionId = new FlowId("someFunctionType", "someflowInstance");

        var storedParameter = "".ToJson();
        
        await store.CreateFunction(
            functionId,
            storedParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.PostponeFunction(
            functionId,
            postponeUntil: DateTime.UtcNow.AddDays(-1).Ticks,
            defaultState: null,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch: 0,
            new ComplimentaryState(() => storedParameter, LeaseLength: 0)
        ).ShouldBeTrueAsync();

        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                leaseLength: TimeSpan.FromMilliseconds(10),
                watchdogCheckFrequency: TimeSpan.FromMilliseconds(10)
            )
        );

        var insideRFuncFlag = new SyncedFlag();
        var completeRFuncFlag = new SyncedFlag();

        functionsRegistry.RegisterAction(
            functionId.Type,
            async (string _) =>
            {
                insideRFuncFlag.Raise();
                await completeRFuncFlag.WaitForRaised();
                return Succeed.WithoutValue;
            }
        );

        await insideRFuncFlag.WaitForRaised();

        var shutdownTask = functionsRegistry.ShutdownGracefully();
        await Task.Delay(10);
        shutdownTask.IsCompleted.ShouldBeFalse();
        
        completeRFuncFlag.Raise();
        await BusyWait.UntilAsync(() => shutdownTask.IsCompleted);
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    [TestMethod]
    public async Task RFunctionsShutdownTaskCompletesWhenInvokedRFuncIsPostponed()
    {
        var flowType = "flowType".ToFlowType();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var store = new InMemoryFunctionStore();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                leaseLength: TimeSpan.FromMilliseconds(10),
                watchdogCheckFrequency: TimeSpan.FromMilliseconds(10)
            )
        );

        var counter = new SyncedCounter();

        var rAction = functionsRegistry.RegisterAction(
            flowType,
            Result (string _) =>
            {
                counter.Increment();
                return Postpone.For(500);
            }
        ).Invoke;

        _ = rAction("instanceId", "1");

        await BusyWait.UntilAsync(() => counter.Current == 1);

        var shutdownTask = functionsRegistry.ShutdownGracefully();
        await Task.Delay(10);
        shutdownTask.IsCompleted.ShouldBeTrue();

        await Task.Delay(700);
        
        counter.Current.ShouldBe(1);

        var sf = await store.GetFunction(new FlowId(flowType, "instanceId"));
        sf!.Status.ShouldBe(Status.Postponed);
            
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
}