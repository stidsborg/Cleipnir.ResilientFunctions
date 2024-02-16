using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
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
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(unhandledExceptionHandler.Catch, postponedCheckFrequency: TimeSpan.FromSeconds(60))
        );

        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
            Result(string _) => Suspend.UntilAfter(0)
        );

        await Should.ThrowAsync<FunctionInvocationSuspendedException>(
            () => rAction.Invoke(functionInstanceId.Value, "hello world")
        );

        await BusyWait.Until(() =>
            store.GetFunction(functionId).SelectAsync(sf => sf?.Status == Status.Suspended)
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
        var functionId = TestFunctionId.Create();
        var (typeId, instanceId) = functionId;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(unhandledExceptionHandler.Catch, postponedCheckFrequency: TimeSpan.FromSeconds(60))
        );
        
        var rFunc = functionsRegistry.RegisterFunc(
            typeId,
            Result<string>(string _) => Suspend.UntilAfter(0)
        );

        await Should.ThrowAsync<FunctionInvocationSuspendedException>(
            () => rFunc.Invoke(instanceId.Value, "hello world")
        );
        
        await BusyWait.Until(() =>
            store.GetFunction(functionId).SelectAsync(sf => sf?.Status == Status.Suspended)
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
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var invocations = 0;
        var rFunc = functionsRegistry.RegisterFunc(
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

        var messagesWriter = rFunc.MessageWriters.For(functionInstanceId); 
        await messagesWriter.AppendMessage("hello multiverse");

        await BusyWait.UntilAsync(() => invocations == 2);
    }
    
    public abstract Task PostponedFunctionIsResumedAfterEventIsAppendedToMessages();
    protected async Task PostponedFunctionIsResumedAfterEventIsAppendedToMessages(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var invocations = 0;
        var rFunc = functionsRegistry.RegisterFunc(
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

        var messagesWriter = rFunc.MessageWriters.For(functionInstanceId); 
        await messagesWriter.AppendMessage("hello multiverse");

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
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var flag = new SyncedFlag();
        var rFunc = functionsRegistry.RegisterFunc<string, string>(
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

        await rFunc.MessageWriters.For(functionInstanceId).AppendMessage("hello universe");
        
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
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var registration = functionsRegistry.RegisterFunc(
            nameof(SuspendedFunctionIsAutomaticallyReInvokedWhenEligibleAndWriteHasTrueBoolFlag),
            async Task<string> (string param, Workflow workflow) =>
            {
                var messages = workflow.Messages;
                var next = await messages.SuspendUntilFirstOfType<string>();
                return next;
            }
        );

        await Should.ThrowAsync<FunctionInvocationSuspendedException>(() =>
            registration.Invoke(functionInstanceId, "hello world")
        );

        var messagesWriter = registration.MessageWriters.For(functionInstanceId);
        await messagesWriter.AppendMessage("hello universe");

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
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var registration = functionsRegistry.RegisterFunc(
            functionTypeId,
            async Task<string> (string param, Workflow workflow) =>
            {
                var messages = workflow.Messages;
                var next = await messages.SuspendUntilFirstOfType<string>();
                return next;
            }
        );

        await Should.ThrowAsync<FunctionInvocationSuspendedException>(() =>
            registration.Invoke(functionInstanceId.Value, "hello world")
        );

        var messagesWriter = registration.MessageWriters.For(functionInstanceId);
        await messagesWriter.AppendMessage("hello universe");

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
    
    public abstract Task StartedChildFuncInvocationPublishesResultSuccessfully();
    protected async Task StartedChildFuncInvocationPublishesResultSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var parentFunctionId = new FunctionId($"ParentFunction{Guid.NewGuid()}", Guid.NewGuid().ToString());

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));

        var child = functionsRegistry.RegisterAction(
            $"ChildFunction{Guid.NewGuid()}",
            inner: async Task (string param, Workflow workflow) =>
            {
                await workflow.PublishMessage(parentFunctionId, param.ToUpper(), workflow.FunctionId.ToString());
            }
        );

        var parent = functionsRegistry.RegisterFunc(
            parentFunctionId.TypeId,
            async Task<string> (string param, Workflow workflow) =>
            {
                var words = param.Split(" ");
                await Task.WhenAll(
                    words.Select((word, i) => child.Schedule($"Child#{i}", $"{i}_{word}"))
                );

                var replies = await workflow
                    .Messages
                    .OfType<string>()
                    .Take(words.Length)
                    .SuspendUntilToList();

                var wordsList = replies
                    .Select(word =>
                    {
                        var split = word.Split("_");
                        return new { Position = int.Parse(split[0]), Word = split[1] };
                    })
                    .OrderBy(a => a.Position)
                    .Select(a => a.Word);
                return string.Join(" ", wordsList);
            });

        await parent.Schedule(parentFunctionId.InstanceId.Value, "hello world and universe");

        var controlPanel = await parent.ControlPanel(parentFunctionId.InstanceId);
        controlPanel.ShouldNotBeNull();
        
        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Succeeded;
        });

        controlPanel.Result.ShouldBe("hello world and universe".ToUpper());
    }
    
    public abstract Task StartedChildActionInvocationPublishesResultSuccessfully();
    protected async Task StartedChildActionInvocationPublishesResultSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var parentFunctionId = new FunctionId($"ParentFunction{Guid.NewGuid()}", Guid.NewGuid().ToString());

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));

        var child = functionsRegistry.RegisterAction(
            $"ChildFunction{Guid.NewGuid()}",
            inner: (string param, Workflow workflow) =>
                workflow.PublishMessage(
                    parentFunctionId,
                    message: "",
                    idempotencyKey: null
                )
        );

        var parent = functionsRegistry.RegisterAction(
            parentFunctionId.TypeId,
            inner: async Task (string param, Workflow workflow) =>
            {
                await child.Schedule("SomeChildInstance#1", "hallo world");
                await child.Schedule("SomeChildInstance#2", "hallo world");
                
                await workflow.Messages.Take(2).SuspendUntilCompletion();
            }
        );

        await parent.Schedule(parentFunctionId.InstanceId.Value, "hello world");

        var controlPanel = await parent.ControlPanel(parentFunctionId.InstanceId);
        controlPanel.ShouldNotBeNull();
        
        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Succeeded;
        });
    }
    
    public abstract Task PublishFromChildActionStressTest();
    protected async Task PublishFromChildActionStressTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var parentFunctionId = new FunctionId($"ParentFunction{Guid.NewGuid()}", Guid.NewGuid().ToString());
        const int numberOfChildren = 100;
        
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));

        var child = functionsRegistry.RegisterAction(
            $"ChildFunction{Guid.NewGuid()}",
            inner: (string param, Workflow workflow) =>
                workflow.PublishMessage(
                    parentFunctionId,
                    message: param,
                    idempotencyKey: $"ChildFunction{Guid.NewGuid()}"
                )
        );

        var parent = functionsRegistry.RegisterFunc(
            parentFunctionId.TypeId,
            inner: async Task<List<string>> (string param, Workflow workflow) =>
            {
                await workflow.Activities.Do("ScheduleChildren", async () =>
                {
                    for (var i = 0; i < numberOfChildren; i++)
                        await child.Schedule($"SomeChildInstance#{i}", i.ToString());
                });
                
                var messages = await workflow.Messages
                    .Take(numberOfChildren)
                    .Select(m => m.ToString()!)
                    .SuspendUntilCompletion();

                return messages;
            }
        );

        await parent.Schedule(parentFunctionId.InstanceId.Value, "hello world");

        var controlPanel = await parent.ControlPanel(parentFunctionId.InstanceId);
        controlPanel.ShouldNotBeNull();
        
        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Succeeded;
        }, maxWait: TimeSpan.FromSeconds(60));

        var result = controlPanel.Result!.ToHashSet();
        for (var i = 0; i < numberOfChildren; i++)
            result.Contains(i.ToString()).ShouldBeTrue();
    }
}