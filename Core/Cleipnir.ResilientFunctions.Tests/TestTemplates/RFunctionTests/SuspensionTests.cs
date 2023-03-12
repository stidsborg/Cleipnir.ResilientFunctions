using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Reactive;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class SuspensionTests
{
    public abstract Task ActionCanBeSuspended();
    protected async Task ActionCanBeSuspended(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(ActionCanBeSuspended).ToFunctionTypeId();
        var functionInstanceId = "functionInstanceId";
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            Result(string _) => Suspend.Until(1)
        );

        await Should.ThrowAsync<FunctionInvocationSuspendedException>(
            () => rAction.Invoke(functionInstanceId, "hello world")
        );

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Suspended);
        sf.SuspendedUntilEventSourceCount.ShouldBe(1);

        var epoch = await store.IsFunctionSuspendedAndEligibleForReInvocation(functionId);
        epoch.ShouldBeNull();
    }
    
    public abstract Task FunctionCanBeSuspended();
    protected async Task FunctionCanBeSuspended(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(FunctionCanBeSuspended).ToFunctionTypeId();
        var functionInstanceId = "functionInstanceId";
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var rFunc = rFunctions.RegisterFunc(
            functionTypeId,
            Result<string>(string _) => Suspend.Until(1)
        );

        await Should.ThrowAsync<FunctionInvocationSuspendedException>(
            () => rFunc.Invoke(functionInstanceId, "hello world")
        );

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Suspended);
        sf.SuspendedUntilEventSourceCount.ShouldBe(1);
        
        var epoch = await store.IsFunctionSuspendedAndEligibleForReInvocation(functionId);
        epoch.ShouldBeNull();
    }
    
    public abstract Task DetectionOfEligibleSuspendedFunctionSucceedsAfterEventAdded();
    protected async Task DetectionOfEligibleSuspendedFunctionSucceedsAfterEventAdded(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(DetectionOfEligibleSuspendedFunctionSucceedsAfterEventAdded).ToFunctionTypeId();
        var functionInstanceId = "functionInstanceId";
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var rFunc = rFunctions.RegisterFunc(
            functionTypeId,
            Result<string>(string _) => Suspend.Until(2)
        );

        await Should.ThrowAsync<FunctionInvocationSuspendedException>(
            () => rFunc.Invoke(functionInstanceId, "hello world")
        );

        (await store.GetEligibleSuspendedFunctions(functionTypeId)).ShouldBeEmpty();

        await rFunc.EventSourceWriters.For(functionInstanceId).AppendEvent("hello universe");

        (await store.GetEligibleSuspendedFunctions(functionTypeId)).ShouldBeEmpty();
        
        await rFunc.EventSourceWriters.For(functionInstanceId).AppendEvent("hello multiverse", reInvokeImmediatelyIfSuspended: false);
        
        var eligibleFunctions = await TaskLinq.ToListAsync(store
                .GetEligibleSuspendedFunctions(functionTypeId));
        
        eligibleFunctions.Count.ShouldBe(1);
        eligibleFunctions[0].InstanceId.ShouldBe(functionInstanceId);
        eligibleFunctions[0].Epoch.ShouldBe(0);
        
        var epoch = await store.IsFunctionSuspendedAndEligibleForReInvocation(functionId);
        epoch.ShouldNotBeNull();
        epoch.Value.ShouldBe(0);
    }
    
    public abstract Task EligibleSuspendedFunctionIsPickedUpByWatchdog();
    protected async Task EligibleSuspendedFunctionIsPickedUpByWatchdog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(EligibleSuspendedFunctionIsPickedUpByWatchdog).ToFunctionTypeId();
        var functionInstanceId = "functionInstanceId";
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions
        (
            store,
            new Settings(unhandledExceptionHandler.Catch, suspensionCheckFrequency: TimeSpan.FromMilliseconds(100))
        );

        var flag = new SyncedFlag();
        var rFunc = rFunctions.RegisterFunc<string, string>(
            functionTypeId,
            Result<string>(_) =>
            {
                if (flag.IsRaised) return "success";
                flag.Raise();
                return Suspend.Until(1);
            });

        await Should.ThrowAsync<FunctionInvocationSuspendedException>(
            () => rFunc.Invoke(functionInstanceId, "hello world")
        );

        await rFunc.EventSourceWriters.For(functionInstanceId).AppendEvent("hello universe");
        
        await BusyWait.Until(
            () => store.GetFunction(functionId).SelectAsync(sf => sf?.Status == Status.Succeeded)
        );
    }
    
    public abstract Task SuspendedFunctionIsAutomaticallyReInvokedWhenEligibleAndWriteHasTrueBoolFlag();
    protected async Task SuspendedFunctionIsAutomaticallyReInvokedWhenEligibleAndWriteHasTrueBoolFlag(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionInstanceId = "functionInstanceId";

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions
        (
            store,
            new Settings(unhandledExceptionHandler.Catch, suspensionCheckFrequency: TimeSpan.FromMilliseconds(100))
        );

        var registration = rFunctions.RegisterFunc(
            nameof(SuspendedFunctionIsAutomaticallyReInvokedWhenEligibleAndWriteHasTrueBoolFlag),
            async Task<string> (string param, Context context) =>
            {
                var eventSource = await context.EventSource;
                var next = await eventSource.SuspendUntilNextOfType<string>();
                return next;
            }
        );

        await Should.ThrowAsync<FunctionInvocationSuspendedException>(() =>
            registration.Invoke(functionInstanceId, "hello world")
        );

        var eventSourceWriter = registration.EventSourceWriters.For(functionInstanceId);
        await eventSourceWriter.AppendEvent("hello universe");

        var controlPanel = await registration.ControlPanel.For(functionInstanceId);
        controlPanel.ShouldNotBeNull();
        
        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Succeeded;
        });

        await controlPanel.Refresh();
        controlPanel.Result.ShouldBe("hello universe");
    }
    
    public abstract Task SuspendedFunctionIsAutomaticallyReInvokedWhenEligibleByWatchdog();
    protected async Task SuspendedFunctionIsAutomaticallyReInvokedWhenEligibleByWatchdog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionInstanceId = "functionInstanceId";

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions
        (
            store,
            new Settings(unhandledExceptionHandler.Catch, suspensionCheckFrequency: TimeSpan.FromMilliseconds(100))
        );

        var registration = rFunctions.RegisterFunc(
            nameof(SuspendedFunctionIsAutomaticallyReInvokedWhenEligibleAndWriteHasTrueBoolFlag),
            async Task<string> (string param, Context context) =>
            {
                var eventSource = await context.EventSource;
                var next = await eventSource.SuspendUntilNextOfType<string>();
                return next;
            }
        );

        await Should.ThrowAsync<FunctionInvocationSuspendedException>(() =>
            registration.Invoke(functionInstanceId, "hello world")
        );

        var eventSourceWriter = registration.EventSourceWriters.For(functionInstanceId);
        await eventSourceWriter.AppendEvent("hello universe", reInvokeImmediatelyIfSuspended: false);

        var controlPanel = await registration.ControlPanel.For(functionInstanceId);
        controlPanel.ShouldNotBeNull();
        
        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Succeeded;
        });

        await controlPanel.Refresh();
        controlPanel.Result.ShouldBe("hello universe");
    }
}