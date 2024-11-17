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
        await BusyWait.Until(() => shutdownTask.IsCompleted);
        rFuncTask1.IsCompletedSuccessfully.ShouldBeTrue();
        rFuncTask2.IsCompletedSuccessfully.ShouldBeTrue();
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
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

        var registration = functionsRegistry.RegisterAction(
            functionId.Type,
            async (string _) =>
            {
                insideRFuncFlag.Raise();
                await completeRFuncFlag.WaitForRaised();
            }
        );
        
        await store.CreateFunction(
            registration.MapToStoredId(functionId), 
            "humanInstanceId",
            param: "".ToJson().ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        ).ShouldBeTrueAsync();

        await insideRFuncFlag.WaitForRaised();

        var shutdownTask = functionsRegistry.ShutdownGracefully();
        await Task.Delay(10);
        shutdownTask.IsCompleted.ShouldBeFalse();
        
        completeRFuncFlag.Raise();
        await BusyWait.Until(() => shutdownTask.IsCompleted);
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    [TestMethod]
    public async Task RFunctionsShutdownTaskOnlyCompletesAfterPostponedWatchDogsRFunctionHasCompleted()
    {
        var store = new InMemoryFunctionStore();
        var functionId = new FlowId("someFunctionType", "someflowInstance");

        var storedParameter = "".ToJson();
        
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                leaseLength: TimeSpan.FromMilliseconds(2_000),
                watchdogCheckFrequency: TimeSpan.FromMilliseconds(100)
            )
        );

        var insideRFuncFlag = new SyncedFlag();
        var completeRFuncFlag = new SyncedFlag();

        var registration = functionsRegistry.RegisterAction(
            functionId.Type,
            async (string _) =>
            {
                insideRFuncFlag.Raise();
                await completeRFuncFlag.WaitForRaised();
                return Succeed.WithoutValue;
            }
        );
        
        await store.CreateFunction(
            registration.MapToStoredId(functionId), 
            "humanInstanceId",
            storedParameter.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        ).ShouldBeTrueAsync();

        await store.PostponeFunction(
            registration.MapToStoredId(functionId),
            postponeUntil: DateTime.UtcNow.AddDays(-1).Ticks,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch: 0,
            new ComplimentaryState(() => storedParameter.ToUtf8Bytes(), LeaseLength: 0)
        ).ShouldBeTrueAsync();


        await insideRFuncFlag.WaitForRaised();

        var shutdownTask = functionsRegistry.ShutdownGracefully();
        await Task.Delay(10);
        shutdownTask.IsCompleted.ShouldBeFalse();
        
        completeRFuncFlag.Raise();
        await BusyWait.Until(() => shutdownTask.IsCompleted);
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
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

        var registration = functionsRegistry.RegisterAction(
            flowType,
            Task<Result> (string _) =>
            {
                counter.Increment();
                return Postpone.For(500).ToResult().ToTask();
            }
        );
        var rAction = registration.Invoke;
        _ = rAction("instanceId", "1");

        await BusyWait.Until(() => counter.Current == 1);

        var shutdownTask = functionsRegistry.ShutdownGracefully();
        await Task.Delay(10);
        shutdownTask.IsCompleted.ShouldBeTrue();

        await Task.Delay(700);
        
        counter.Current.ShouldBe(1);

        var sf = await store.GetFunction(registration.MapToStoredId(new FlowId(flowType, "instanceId")));
        sf!.Status.ShouldBe(Status.Postponed);
            
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
}