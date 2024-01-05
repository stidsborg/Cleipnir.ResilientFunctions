using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Reactive;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
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
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            Result(string _) => Suspend.UntilAfter(0)
        );

        await Should.ThrowAsync<FunctionInvocationSuspendedException>(
            () => rAction.Invoke(functionInstanceId.Value, "hello world")
        );

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Suspended);
        (sf.Epoch is 0).ShouldBeTrue();
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
            Result<string>(string _) => Suspend.UntilAfter(0)
        );

        await Should.ThrowAsync<FunctionInvocationSuspendedException>(
            () => rFunc.Invoke(functionInstanceId, "hello world")
        );

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Suspended);
        (sf.Epoch is 0).ShouldBeTrue();
    }
    
    public abstract Task DetectionOfEligibleSuspendedFunctionSucceedsAfterEventAdded();
    protected async Task DetectionOfEligibleSuspendedFunctionSucceedsAfterEventAdded(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var invocations = 0;
        var rFunc = rFunctions.RegisterFunc(
            functionTypeId,
            Result<string>(string _) =>
            {
                if (invocations == 0)
                {
                    invocations++;
                    return Suspend.UntilAfter(0);
                }

                invocations++;
                return Result.SucceedWithValue("completed");
            });

        await Should.ThrowAsync<FunctionInvocationSuspendedException>(
            () => rFunc.Invoke(functionInstanceId.Value, "hello world")
        );

        await Task.Delay(250);

        var eventSourceWriter = rFunc.EventSourceWriters.For(functionInstanceId); 
        await eventSourceWriter.AppendEvent("hello multiverse");

        await BusyWait.UntilAsync(() => invocations == 2);
    }
    
    public abstract Task PostponedFunctionIsResumedAfterEventIsAppendedToEventSource();
    protected async Task PostponedFunctionIsResumedAfterEventIsAppendedToEventSource(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var invocations = 0;
        var rFunc = rFunctions.RegisterFunc(
            functionTypeId,
            Result<string>(string _) =>
            {
                if (invocations == 0)
                {
                    invocations++;
                    return Postpone.For(TimeSpan.FromHours(1));
                }

                invocations++;
                return Result.SucceedWithValue("completed");
            });

        await Should.ThrowAsync<FunctionInvocationPostponedException>(
            () => rFunc.Invoke(functionInstanceId.Value, "hello world")
        );

        await Task.Delay(250);

        var eventSourceWriter = rFunc.EventSourceWriters.For(functionInstanceId); 
        await eventSourceWriter.AppendEvent("hello multiverse");

        await BusyWait.UntilAsync(() => invocations == 2);
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
            new Settings(unhandledExceptionHandler.Catch)
        );

        var flag = new SyncedFlag();
        var rFunc = rFunctions.RegisterFunc<string, string>(
            functionTypeId,
            Result<string>(_) =>
            {
                if (flag.IsRaised) return "success";
                flag.Raise();
                return Suspend.UntilAfter(0);
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
            new Settings(unhandledExceptionHandler.Catch)
        );

        var registration = rFunctions.RegisterFunc(
            nameof(SuspendedFunctionIsAutomaticallyReInvokedWhenEligibleAndWriteHasTrueBoolFlag),
            async Task<string> (string param, Context context) =>
            {
                var eventSource = context.EventSource;
                var next = await eventSource.SuspendUntilNextOfType<string>();
                return next;
            }
        );

        await Should.ThrowAsync<FunctionInvocationSuspendedException>(() =>
            registration.Invoke(functionInstanceId, "hello world")
        );

        var eventSourceWriter = registration.EventSourceWriters.For(functionInstanceId);
        await eventSourceWriter.AppendEvent("hello universe");

        var controlPanel = await registration.ControlPanel(functionInstanceId);
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
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var registration = rFunctions.RegisterFunc(
            functionTypeId,
            async Task<string> (string param, Context context) =>
            {
                var eventSource = context.EventSource;
                var next = await eventSource.SuspendUntilNextOfType<string>();
                return next;
            }
        );

        await Should.ThrowAsync<FunctionInvocationSuspendedException>(() =>
            registration.Invoke(functionInstanceId.Value, "hello world")
        );

        var eventSourceWriter = registration.EventSourceWriters.For(functionInstanceId);
        await eventSourceWriter.AppendEvent("hello universe");

        var controlPanel = await registration.ControlPanel(functionInstanceId);
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