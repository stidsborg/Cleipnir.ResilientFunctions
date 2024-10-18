using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Messaging.Utils;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;
using FlagPosition = Cleipnir.ResilientFunctions.Tests.Utils.FlagPosition;
using SyncedFlag = Cleipnir.ResilientFunctions.Tests.Utils.SyncedFlag;

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
            Task<Result> (string _) => throw new SuspendInvocationException()
        );

        await Should.ThrowAsync<InvocationSuspendedException>(
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
            Task<Result<string>>(string _) => Suspend.Invocation.ToResult<string>().ToTask()
        );

        await Should.ThrowAsync<InvocationSuspendedException>(
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
            Task<Result<string>> (string _) =>
            {
                if (invocations == 0)
                {
                    invocations++;
                    return Suspend.Invocation.ToResult<string>().ToTask();
                }

                invocations++;
                return Result.SucceedWithValue("completed").ToTask();
            });

        await Should.ThrowAsync<InvocationSuspendedException>(
            () => rFunc.Invoke(flowInstance.Value, "hello world")
        );

        await Task.Delay(250);

        var messagesWriter = rFunc.MessageWriters.For(flowInstance); 
        await messagesWriter.AppendMessage("hello multiverse");

        await BusyWait.Until(() => invocations == 2);
        
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
            Task<Result<string>> (string _) =>
            {
                if (invocations == 0)
                {
                    invocations++;
                    return Postpone.For(TimeSpan.FromHours(1)).ToResult<string>().ToTask();
                }

                invocations++;
                return Result.SucceedWithValue("completed").ToTask();
            });

        await Should.ThrowAsync<InvocationPostponedException>(
            () => rFunc.Invoke(flowInstance.Value, "hello world")
        );

        await Task.Delay(250);

        var messagesWriter = rFunc.MessageWriters.For(flowInstance); 
        await messagesWriter.AppendMessage("hello multiverse");

        await BusyWait.Until(() => invocations == 2);
        
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
            Task<Result<string>> (_) =>
            {
                if (flag.IsRaised) return Result.SucceedWithValue("success").ToTask();
                flag.Raise();
                return Suspend.Invocation.ToResult<string>().ToTask();
            });

        await Should.ThrowAsync<InvocationSuspendedException>(
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

        await Should.ThrowAsync<InvocationSuspendedException>(() =>
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

        await Should.ThrowAsync<InvocationSuspendedException>(() =>
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

        FuncRegistration<string, string>? parent = null;
        var child = functionsRegistry.RegisterAction(
            $"ChildFunction{Guid.NewGuid()}",
            inner: async Task (string param, Workflow workflow) =>
            {
                await parent!.MessageWriters
                    .For(parentFunctionId.Instance)
                    .AppendMessage(param.ToUpper(), workflow.FlowId.ToString()
                );
            }
        );

        parent = functionsRegistry.RegisterFunc(
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

        ActionRegistration<string>? parent = null;
        var child = functionsRegistry.RegisterAction(
            $"ChildFunction{Guid.NewGuid()}",
            inner: (string param, Workflow workflow) =>
                parent!.MessageWriters.For(parentFunctionId.Instance).AppendMessage("")
        );

        parent = functionsRegistry.RegisterAction(
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

        FuncRegistration<string, List<string>>? parent = null;
        var child = functionsRegistry.RegisterAction(
            $"ChildFunction{Guid.NewGuid()}",
            inner: (string param) =>
                parent!.MessageWriters
                    .For(parentFunctionId.Instance)
                    .AppendMessage(param, idempotencyKey: $"ChildFunction{Guid.NewGuid()}")
        );

        parent = functionsRegistry.RegisterFunc(
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
    
    public abstract Task InterruptCountIsUpdatedWhenMaxWaitDetectsIt();
    protected async Task InterruptCountIsUpdatedWhenMaxWaitDetectsIt(Task<IFunctionStore> storeTask)
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

        var syncFlag = new SyncedFlag();
        var suspendedFlag = new SyncedFlag();
        
        var registration = functionsRegistry.RegisterParamless(
            flowType,
            inner: async Task (workflow) =>
            {
                syncFlag.Raise();
                try
                {
                    await workflow.Messages.FirstOfType<int>(maxWait: TimeSpan.FromSeconds(2));
                    await workflow.Messages.FirstOfType<string>(maxWait: TimeSpan.FromMilliseconds(100));
                }
                catch (SuspendInvocationException)
                {
                    suspendedFlag.Raise();
                }
            }
        );
        
        await registration.Schedule(flowInstance);
        await syncFlag.WaitForRaised();

        await registration.SendMessage(flowInstance, message: 32);
        var controlPanel = await registration.ControlPanel(flowInstance);
        controlPanel.ShouldNotBeNull();

        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status != Status.Executing;
        });
           
        suspendedFlag.Position.ShouldBe(FlagPosition.Raised);
    }
    
    public abstract Task SuspendedFlowIsRestartedAfterInterrupt();
    protected async Task SuspendedFlowIsRestartedAfterInterrupt(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var id = TestFlowId.Create();
        var (flowType, flowInstance) = id;

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var syncFlag = new SyncedFlag();
        var registration = functionsRegistry.RegisterParamless(
            flowType,
            inner: Task (workflow) =>
            {
                if (syncFlag.IsRaised)
                    return Task.CompletedTask;
                
                syncFlag.Raise();
                throw new SuspendInvocationException();
            }
        );
        
        await registration.Schedule(flowInstance);
        await syncFlag.WaitForRaised();

        var controlPanel = await registration.ControlPanel(flowInstance).ShouldNotBeNullAsync();

        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Suspended;
        });

        await store.Interrupt(id, onlyIfExecuting: false);
        
        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Succeeded;
        });
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task ExecutingFlowIsReExecutedWhenSuspendedAfterInterrupt();
    protected async Task ExecutingFlowIsReExecutedWhenSuspendedAfterInterrupt(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var id = TestFlowId.Create();
        var (flowType, flowInstance) = id;

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var insideFlowFlag = new SyncedFlag();
        var canContinueFlag = new SyncedFlag();
        var executingAgainFlag = new SyncedFlag();
        
        var registration = functionsRegistry.RegisterParamless(
            flowType,
            inner: async Task (workflow) =>
            {
                if (insideFlowFlag.IsRaised)
                {
                    executingAgainFlag.Raise();
                    return;
                }

                insideFlowFlag.Raise();
                await canContinueFlag.WaitForRaised();
                throw new SuspendInvocationException();
            }
        );
        
        await registration.Schedule(flowInstance);
        await insideFlowFlag.WaitForRaised();

        await store.Interrupt(id, onlyIfExecuting: true);
        canContinueFlag.Raise();
        
        await executingAgainFlag.WaitForRaised();
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
}