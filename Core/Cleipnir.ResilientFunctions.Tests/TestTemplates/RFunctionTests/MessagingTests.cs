﻿using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Reactive;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class MessagingTests
{
    public abstract Task FunctionCompletesAfterAwaitedMessageIsReceived();
    public async Task FunctionCompletesAfterAwaitedMessageIsReceived(Task<IFunctionStore> functionStore)
    {
        var store = await functionStore;
        
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler.Catch, messagesDefaultMaxWaitForCompletion: TimeSpan.MaxValue)
        );

        var rAction = functionsRegistry.RegisterFunc(
            nameof(FunctionCompletesAfterAwaitedMessageIsReceived),
            inner: async Task<string> (string _, Workflow workflow) =>
            {
                var messages = workflow.Messages;
                return await messages.OfType<string>().First();
            }
        );

        var invocationTask = rAction.Invoke("instanceId", "");
        await Task.Delay(100);
        invocationTask.IsCompleted.ShouldBeFalse();
        
        var messagesWriter = rAction.MessageWriters.For("instanceId");
        await messagesWriter.AppendMessage("hello world");
        var result = await invocationTask;
        result.ShouldBe("hello world");
        
        unhandledExceptionHandler.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task FunctionIsSuspendedWhenAwaitedMessageDoesNotAlreadyExist();
    public async Task FunctionIsSuspendedWhenAwaitedMessageDoesNotAlreadyExist(Task<IFunctionStore> functionStore)
    {
        var store = await functionStore;

        var functionId = TestFunctionId.Create();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));

        var rAction = functionsRegistry.RegisterFunc(
            functionId.TypeId,
            inner: async Task<string> (string _, Workflow workflow) =>
            {
                var messages = workflow.Messages;
                return await messages.SuspendUntilFirstOfType<string>();
            }
        );

        await Should.ThrowAsync<FunctionInvocationSuspendedException>(() =>
            rAction.Invoke(functionId.InstanceId.Value, "")
        );
        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Suspended);
        
        unhandledExceptionHandler.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task TimeoutEventCausesSuspendedFunctionToBeReInvoked();
    public async Task TimeoutEventCausesSuspendedFunctionToBeReInvoked(Task<IFunctionStore> functionStore)
    {
        var store = await functionStore;

        var functionId = new FunctionId(nameof(TimeoutEventCausesSuspendedFunctionToBeReInvoked),"instanceId");
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));

        var rFunc = functionsRegistry.RegisterFunc(
            functionId.TypeId,
            inner: async Task<Tuple<bool, string>> (string _, Workflow workflow) =>
            {
                var messages = workflow.Messages;

                var timeoutOption = await messages
                    .OfType<string>()
                    .TakeUntilTimeout("timeoutId1", expiresIn: TimeSpan.FromMilliseconds(250))
                    .SuspendUntilFirstOrNone();
                
                var timeoutEvent = messages
                    .OfType<TimeoutEvent>()
                    .Existing(out var __)
                    .SingleOrDefault();
                
                return Tuple.Create(timeoutEvent != null && !timeoutOption.HasValue, timeoutEvent?.TimeoutId ?? "");
            }
        );

        await Should.ThrowAsync<FunctionInvocationSuspendedException>(() =>
            rFunc.Invoke(functionId.InstanceId.Value, "")
        );

        var controlPanel = await rFunc.ControlPanel("instanceId");
        controlPanel.ShouldNotBeNull();

        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Succeeded;
        });


        controlPanel.Result.ShouldNotBeNull();
        var (success, timeoutId) = controlPanel.Result;
        success.ShouldBeTrue();
        timeoutId.ShouldBe("timeoutId1");
        
        unhandledExceptionHandler.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ScheduleInvocationWithPublishResultToSpecifiedFunctionId();
    public async Task ScheduleInvocationWithPublishResultToSpecifiedFunctionId(Task<IFunctionStore> functionStore)
    {
        var store = await functionStore;

        var parentFunctionId = TestFunctionId.Create();
        var childFunctionId = TestFunctionId.Create();
        
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));

        var child = functionsRegistry.RegisterAction(
            childFunctionId.TypeId,
            inner: Task (string _, Workflow workflow) => workflow.SendMessage(parentFunctionId, "hello world", idempotencyKey: null)
        );

        var parent = functionsRegistry.RegisterFunc(
            parentFunctionId.TypeId,
            inner: async Task<string> (string _, Workflow workflow) =>
            {
                await child.Schedule(childFunctionId.InstanceId.Value, param: "stuff");
                return await workflow.Messages.SuspendUntilFirstOfType<string>();
            }
        );
        
        await Should.ThrowAsync<FunctionInvocationSuspendedException>(() =>
            parent.Invoke(parentFunctionId.InstanceId.Value, "")
        );
        
        var controlPanel = await parent.ControlPanel(parentFunctionId.InstanceId);
        controlPanel.ShouldNotBeNull();

        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Succeeded;
        });
        
        controlPanel.Result.ShouldNotBeNull();
        var functionCompletion = controlPanel.Result;
        functionCompletion.ShouldBe("hello world");
        
        unhandledExceptionHandler.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task IsWorkflowRunningSubscriptionPropertyTurnsFalseAfterWorkflowInvocationHasCompleted();
    public async Task IsWorkflowRunningSubscriptionPropertyTurnsFalseAfterWorkflowInvocationHasCompleted(Task<IFunctionStore> functionStore)
    {
        var store = await functionStore;

        var (typeId, instanceId) = TestFunctionId.Create();
        
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));

        ISubscription? subscription = null;
        
        var registration = functionsRegistry.RegisterAction(
            typeId,
            inner: void (string _, Workflow workflow) =>
            {
                subscription = workflow.Messages.Subscribe(onNext: _ => { }, onCompletion: () => { }, onError: _ => { });
                subscription.IsWorkflowRunning.ShouldBeTrue();
            }
        );

        await registration.Invoke(instanceId.ToString(), param: "test");

        subscription.ShouldNotBeNull();
        subscription.IsWorkflowRunning.ShouldBeFalse();

        var controlPanel = await registration.ControlPanel(instanceId);
        controlPanel.ShouldNotBeNull();

        await controlPanel.ReInvoke();
        subscription.ShouldNotBeNull();
        subscription.IsWorkflowRunning.ShouldBeFalse();
        
        unhandledExceptionHandler.ThrownExceptions.ShouldBeEmpty();
    }
}