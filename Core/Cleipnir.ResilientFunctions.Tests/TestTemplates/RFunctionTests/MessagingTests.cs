using System;
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
        
        var messagesWriter = rAction.MessageWriters.For("instanceId".ToFlowInstance());
        await messagesWriter.AppendMessage("hello world");
        var result = await invocationTask;
        result.ShouldBe("hello world");
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task FunctionIsSuspendedWhenAwaitedMessageDoesNotAlreadyExist();
    public async Task FunctionIsSuspendedWhenAwaitedMessageDoesNotAlreadyExist(Task<IFunctionStore> functionStore)
    {
        var store = await functionStore;

        var functionId = TestFlowId.Create();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));

        var rAction = functionsRegistry.RegisterFunc(
            functionId.Type,
            inner: async Task<string> (string _, Workflow workflow) =>
            {
                var messages = workflow.Messages;
                return await messages.FirstOfType<string>(maxWait: TimeSpan.Zero);
            }
        );

        await Should.ThrowAsync<InvocationSuspendedException>(() =>
            rAction.Invoke(functionId.Instance.Value, "")
        );
        var sf = await store.GetFunction(rAction.MapToStoredId(functionId.Instance));
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Suspended);
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task TimeoutEventCausesSuspendedFunctionToBeReInvoked();
    public async Task TimeoutEventCausesSuspendedFunctionToBeReInvoked(Task<IFunctionStore> functionStore)
    {
        var store = await functionStore;

        var functionId = new FlowId(nameof(TimeoutEventCausesSuspendedFunctionToBeReInvoked),"instanceId");
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));

        var rFunc = functionsRegistry.RegisterFunc(
            functionId.Type,
            inner: async Task<Tuple<bool, string>> (string _, Workflow workflow) =>
            {
                var messages = workflow.Messages;

                var timeoutOption = await messages
                    .TakeUntilTimeout("timeoutId1", expiresIn: TimeSpan.FromMilliseconds(250))
                    .OfType<string>()
                    .FirstOrNone(TimeSpan.Zero);
                
                var timeoutEvent = messages
                    .OfType<TimeoutEvent>()
                    .Existing(out var __)
                    .SingleOrDefault();
                
                return Tuple.Create(timeoutEvent != null && !timeoutOption.HasValue, timeoutEvent?.TimeoutId.Id ?? "");
            }
        );

        await Should.ThrowAsync<InvocationSuspendedException>(() =>
            rFunc.Invoke(functionId.Instance.Value, "")
        );

        var controlPanel = await rFunc.ControlPanel("instanceId");
        controlPanel.ShouldNotBeNull();

        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Succeeded;
        }, maxWait: TimeSpan.FromSeconds(10));


        controlPanel.Result.ShouldNotBeNull();
        var (success, timeoutId) = controlPanel.Result;
        success.ShouldBeTrue();
        timeoutId.ShouldBe("timeoutId1");
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task ScheduleInvocationWithPublishResultToSpecifiedFunctionId();
    public async Task ScheduleInvocationWithPublishResultToSpecifiedFunctionId(Task<IFunctionStore> functionStore)
    {
        var store = await functionStore;

        var parentFunctionId = TestFlowId.Create();
        var childFunctionId = TestFlowId.Create();
        
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));

        FuncRegistration<string, string>? parent = null;
        
        var child = functionsRegistry.RegisterAction(
            childFunctionId.Type,
            inner: Task (string _) => parent!
                .MessageWriters
                .For(parentFunctionId.Instance)
                .AppendMessage("hello world")
        );

        parent = functionsRegistry.RegisterFunc(
            parentFunctionId.Type,
            inner: async Task<string> (string _, Workflow workflow) =>
            {
                await child.Schedule(childFunctionId.Instance.Value, param: "stuff");
                return await workflow.Messages.FirstOfType<string>(maxWait: TimeSpan.Zero);
            }
        );

        await parent.Schedule(parentFunctionId.Instance.Value, "");
        
        var controlPanel = await parent.ControlPanel(parentFunctionId.Instance);
        controlPanel.ShouldNotBeNull();

        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Succeeded;
        });
        
        controlPanel.Result.ShouldNotBeNull();
        var functionCompletion = controlPanel.Result;
        functionCompletion.ShouldBe("hello world");
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task IsWorkflowRunningSubscriptionPropertyTurnsFalseAfterWorkflowInvocationHasCompleted();
    public async Task IsWorkflowRunningSubscriptionPropertyTurnsFalseAfterWorkflowInvocationHasCompleted(Task<IFunctionStore> functionStore)
    {
        var store = await functionStore;

        var (typeId, instanceId) = TestFlowId.Create();
        
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));

        ISubscription? subscription = null;
        
        var registration = functionsRegistry.RegisterAction(
            typeId,
            inner: Task (string _, Workflow workflow) =>
            {
                subscription = workflow.Messages.Subscribe(onNext: _ => { }, onCompletion: () => { }, onError: _ => { });
                subscription.IsWorkflowRunning.ShouldBeTrue();
                return Task.CompletedTask;
            }
        );

        await registration.Invoke(instanceId.ToString(), param: "test");

        subscription.ShouldNotBeNull();
        subscription.IsWorkflowRunning.ShouldBeFalse();

        var controlPanel = await registration.ControlPanel(instanceId);
        controlPanel.ShouldNotBeNull();

        await controlPanel.Restart();
        subscription.ShouldNotBeNull();
        subscription.IsWorkflowRunning.ShouldBeFalse();
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
}