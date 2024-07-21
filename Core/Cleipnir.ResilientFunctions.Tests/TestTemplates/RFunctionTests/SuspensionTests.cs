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
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(unhandledExceptionHandler.Catch, watchdogCheckFrequency: TimeSpan.FromSeconds(60))
        );

        var rAction = functionsRegistry.RegisterAction(
            flowType,
            Result(string _) => Suspend.While(0)
        );

        await Should.ThrowAsync<FunctionInvocationSuspendedException>(
            () => rAction.Invoke(flowInstance.Value, "hello world")
        );

        await BusyWait.Until(() =>
            store.GetFunction(functionId).SelectAsync(sf => sf?.Status == Status.Suspended)
        );
        
        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Suspended);
        (sf.Epoch is 0).ShouldBeTrue();
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task FunctionCanBeSuspended();
    protected async Task FunctionCanBeSuspended(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (typeId, instanceId) = functionId;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(unhandledExceptionHandler.Catch, watchdogCheckFrequency: TimeSpan.FromSeconds(60))
        );
        
        var rFunc = functionsRegistry.RegisterFunc(
            typeId,
            Result<string>(string _) => Suspend.While(0)
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
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task DetectionOfEligibleSuspendedFunctionSucceedsAfterEventAdded();
    protected async Task DetectionOfEligibleSuspendedFunctionSucceedsAfterEventAdded(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var invocations = 0;
        var rFunc = functionsRegistry.RegisterFunc(
            flowType,
            Result<string>(string _) =>
            {
                if (invocations == 0)
                {
                    invocations++;
                    return Suspend.While(0);
                }

                invocations++;
                return Result.SucceedWithValue("completed");
            });

        await Should.ThrowAsync<FunctionInvocationSuspendedException>(
            () => rFunc.Invoke(flowInstance.Value, "hello world")
        );

        await Task.Delay(250);

        var messagesWriter = rFunc.MessageWriters.For(flowInstance); 
        await messagesWriter.AppendMessage("hello multiverse");

        await BusyWait.UntilAsync(() => invocations == 2);
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task PostponedFunctionIsResumedAfterEventIsAppendedToMessages();
    protected async Task PostponedFunctionIsResumedAfterEventIsAppendedToMessages(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var invocations = 0;
        var rFunc = functionsRegistry.RegisterFunc(
            flowType,
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
            () => rFunc.Invoke(flowInstance.Value, "hello world")
        );

        await Task.Delay(250);

        var messagesWriter = rFunc.MessageWriters.For(flowInstance); 
        await messagesWriter.AppendMessage("hello multiverse");

        await BusyWait.UntilAsync(() => invocations == 2);
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task EligibleSuspendedFunctionIsPickedUpByWatchdog();
    protected async Task EligibleSuspendedFunctionIsPickedUpByWatchdog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowType = nameof(EligibleSuspendedFunctionIsPickedUpByWatchdog).ToFlowType();
        var flowInstance = "flowInstance";
        var functionId = new FlowId(flowType, flowInstance);
        
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var flag = new SyncedFlag();
        var rFunc = functionsRegistry.RegisterFunc<string, string>(
            flowType,
            Result<string>(_) =>
            {
                if (flag.IsRaised) return "success";
                flag.Raise();
                return Suspend.While(0);
            });

        await Should.ThrowAsync<FunctionInvocationSuspendedException>(
            () => rFunc.Invoke(flowInstance, "hello world")
        );

        await rFunc.MessageWriters.For(flowInstance).AppendMessage("hello universe");
        
        await BusyWait.Until(
            () => store.GetFunction(functionId).SelectAsync(sf => sf?.Status == Status.Succeeded)
        );
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task SuspendedFunctionIsAutomaticallyReInvokedWhenEligibleAndWriteHasTrueBoolFlag();
    protected async Task SuspendedFunctionIsAutomaticallyReInvokedWhenEligibleAndWriteHasTrueBoolFlag(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowInstance = "flowInstance";

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
                var next = await messages.FirstOfType<string>(TimeSpan.Zero);
                return next;
            }
        );

        await Should.ThrowAsync<FunctionInvocationSuspendedException>(() =>
            registration.Invoke(flowInstance, "hello world")
        );

        var messagesWriter = registration.MessageWriters.For(flowInstance);
        await messagesWriter.AppendMessage("hello universe");

        var controlPanel = await registration.ControlPanel(flowInstance);
        controlPanel.ShouldNotBeNull();
        
        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Succeeded;
        });

        await controlPanel.Refresh();
        controlPanel.Result.ShouldBe("hello universe");
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task SuspendedFunctionIsAutomaticallyReInvokedWhenEligibleByWatchdog();
    protected async Task SuspendedFunctionIsAutomaticallyReInvokedWhenEligibleByWatchdog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var registration = functionsRegistry.RegisterFunc(
            flowType,
            async Task<string> (string param, Workflow workflow) =>
            {
                var messages = workflow.Messages;
                var next = await messages.FirstOfType<string>(TimeSpan.Zero);
                return next;
            }
        );

        await Should.ThrowAsync<FunctionInvocationSuspendedException>(() =>
            registration.Invoke(flowInstance.Value, "hello world")
        );

        var messagesWriter = registration.MessageWriters.For(flowInstance);
        await messagesWriter.AppendMessage("hello universe");

        var controlPanel = await registration.ControlPanel(flowInstance);
        controlPanel.ShouldNotBeNull();
        
        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Succeeded;
        });

        await controlPanel.Refresh();
        controlPanel.Result.ShouldBe("hello universe");
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task ParamlessFunctionWithPrefilledMessageCompletes();
    protected async Task ParamlessFunctionWithPrefilledMessageCompletes(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var syncedValue = new Synced<string>();
        
        var registration = functionsRegistry.RegisterParamless(
            flowType,
            inner: async Task (workflow) =>
            {
                var msg = await workflow.Messages.FirstOfType<string>();
                syncedValue.Value = msg;
            }
        );

        await registration.MessageWriters.For(flowInstance.Value).AppendMessage("Hello!");
        await registration.Schedule(flowInstance);

        var controlPanel = await registration.ControlPanel(flowInstance);
        controlPanel.ShouldNotBeNull();
        await controlPanel.WaitForCompletion();
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task StartedChildFuncInvocationPublishesResultSuccessfully();
    protected async Task StartedChildFuncInvocationPublishesResultSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var parentFunctionId = new FlowId($"ParentFunction{Guid.NewGuid()}", Guid.NewGuid().ToString());

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));

        var child = functionsRegistry.RegisterAction(
            $"ChildFunction{Guid.NewGuid()}",
            inner: async Task (string param, Workflow workflow) =>
            {
                await workflow.SendMessage(parentFunctionId, param.ToUpper(), workflow.FlowId.ToString());
            }
        );

        var parent = functionsRegistry.RegisterFunc(
            parentFunctionId.Type,
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
                    .ToList(maxWait: TimeSpan.Zero);

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

        await parent.Schedule(parentFunctionId.Instance.Value, "hello world and universe");

        var controlPanel = await parent.ControlPanel(parentFunctionId.Instance);
        controlPanel.ShouldNotBeNull();
        
        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Succeeded;
        });

        controlPanel.Result.ShouldBe("hello world and universe".ToUpper());
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task StartedChildActionInvocationPublishesResultSuccessfully();
    protected async Task StartedChildActionInvocationPublishesResultSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var parentFunctionId = new FlowId($"ParentFunction{Guid.NewGuid()}", Guid.NewGuid().ToString());

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));

        var child = functionsRegistry.RegisterAction(
            $"ChildFunction{Guid.NewGuid()}",
            inner: (string param, Workflow workflow) =>
                workflow.SendMessage(
                    parentFunctionId,
                    message: "",
                    idempotencyKey: null
                )
        );

        var parent = functionsRegistry.RegisterAction(
            parentFunctionId.Type,
            inner: async Task (string param, Workflow workflow) =>
            {
                await child.Schedule("SomeChildInstance#1", "hallo world");
                await child.Schedule("SomeChildInstance#2", "hallo world");
                
                await workflow.Messages.Take(2).Completion(maxWait: TimeSpan.Zero);
            }
        );

        await parent.Schedule(parentFunctionId.Instance.Value, "hello world");

        var controlPanel = await parent.ControlPanel(parentFunctionId.Instance);
        controlPanel.ShouldNotBeNull();
        
        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Succeeded;
        });
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task PublishFromChildActionStressTest();
    protected async Task PublishFromChildActionStressTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var parentFunctionId = new FlowId($"ParentFunction{Guid.NewGuid()}", Guid.NewGuid().ToString());
        const int numberOfChildren = 100;
        
        using var functionsRegistry = new FunctionsRegistry(store);

        var child = functionsRegistry.RegisterAction(
            $"ChildFunction{Guid.NewGuid()}",
            inner: (string param, Workflow workflow) =>
                workflow.SendMessage(
                    parentFunctionId,
                    message: param,
                    idempotencyKey: $"ChildFunction{Guid.NewGuid()}"
                )
        );

        var parent = functionsRegistry.RegisterFunc(
            parentFunctionId.Type,
            inner: async Task<List<string>> (string param, Workflow workflow) =>
            {
                await workflow.Effect.Capture("ScheduleChildren", async () =>
                {
                    for (var i = 0; i < numberOfChildren; i++)
                        await child.Schedule($"SomeChildInstance#{i}", i.ToString());
                });
                
                var messages = await workflow.Messages
                    .Take(numberOfChildren)
                    .Select(m => m.ToString()!)
                    .Completion(maxWait: TimeSpan.Zero);

                return messages;
            }
        );

        await parent.Schedule(parentFunctionId.Instance.Value, "hello world");

        var controlPanel = await parent.ControlPanel(parentFunctionId.Instance);
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