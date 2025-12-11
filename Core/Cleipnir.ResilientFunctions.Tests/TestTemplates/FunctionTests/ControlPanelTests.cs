using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;
using static Cleipnir.ResilientFunctions.Storage.CrudOperation;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.FunctionTests;

public abstract class ControlPanelTests
{
    public abstract Task ExistingActionCanBeDeletedFromControlPanel();
    protected async Task ExistingActionCanBeDeletedFromControlPanel(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            async (string _, Workflow workflow) =>
            {
                var (effect, messages) = workflow;
                await effect.CreateOrGet("alias", 123);
                await messages.AppendMessage("Message");
                await workflow.Messages.FlowRegisteredTimeouts.RegisterTimeout(1, expiresAt: DateTime.UtcNow.AddDays(1), publishMessage: true);
            }
        );
        
        await rAction.Invoke(flowInstance.Value, "");

        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        await controlPanel.Delete();
        
        await Should.ThrowAsync<UnexpectedStateException>(controlPanel.Refresh());

        var storedId = rAction.MapToStoredId(functionId.Instance);
        await store.GetFunction(storedId).ShouldBeNullAsync();

        await store
            .MessageStore
            .GetMessages(storedId, skip: 0)
            .SelectAsync(messages => messages.Count)
            .ShouldBeAsync(0);

        await store
            .EffectsStore
            .GetEffectResults(storedId)
            .SelectAsync(effects => effects.Count())
            .ShouldBeAsync(0);

        await store
            .EffectsStore
            .GetEffectResults(storedId)
            .SelectAsync(states => states.Count())
            .ShouldBeAsync(0);
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task ExistingFunctionCanBeDeletedFromControlPanel();
    protected async Task ExistingFunctionCanBeDeletedFromControlPanel(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var rFunc = functionsRegistry.RegisterFunc(
            flowType,
            async Task<string>(string _, Workflow workflow) =>
            {
                var (effect, messages) = workflow;
                await effect.CreateOrGet("alias", 123);
                await messages.AppendMessage("Message");
                await workflow.Messages.FlowRegisteredTimeouts.RegisterTimeout(1, expiresAt: DateTime.UtcNow.AddDays(1), publishMessage: true);
                return "hello";
            });
        
        await rFunc.Invoke(flowInstance.Value, "");

        var controlPanel = await rFunc.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        await controlPanel.Delete();

        await Should.ThrowAsync<UnexpectedStateException>(controlPanel.Refresh());

        var storedId = rFunc.MapToStoredId(functionId.Instance);
        await store.GetFunction(storedId).ShouldBeNullAsync();
        
        await store
            .MessageStore
            .GetMessages(storedId, skip: 0)
            .SelectAsync(messages => messages.Count)
            .ShouldBeAsync(0);

        await store
            .EffectsStore
            .GetEffectResults(storedId)
            .SelectAsync(effects => effects.Count())
            .ShouldBeAsync(0);

        await store
            .EffectsStore
            .GetEffectResults(storedId)
            .SelectAsync(states => states.Count())
            .ShouldBeAsync(0);
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task PostponingExistingActionFromControlPanelSucceeds();
    protected async Task PostponingExistingActionFromControlPanelSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();

        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            Task (string _) => throw new Exception("oh no")
        );
        
        await Should.ThrowAsync<Exception>(() => rAction.Invoke(flowInstance.Value, ""));

        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Failed);
        controlPanel.FatalWorkflowException.ShouldNotBeNull();

        var postponeUntil = DateTime.UtcNow.AddDays(1);
        await controlPanel.Postpone(postponeUntil);

        await controlPanel.Refresh();
        controlPanel.Status.ShouldBe(Status.Postponed);
        controlPanel.PostponedUntil.ShouldNotBeNull();
        controlPanel.PostponedUntil.ShouldBe(postponeUntil);
        
        var sf = await store.GetFunction(rAction.MapToStoredId(functionId.Instance));
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Postponed);
        sf.Expires.ShouldBe(postponeUntil.Ticks);
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task PostponingExistingFunctionFromControlPanelSucceeds();
    protected async Task PostponingExistingFunctionFromControlPanelSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var rFunc = functionsRegistry.RegisterFunc<string, string>(
            flowType,
            Task<string> (_) => throw new Exception("oh no")
        );
        
        await Should.ThrowAsync<Exception>(() => rFunc.Invoke(flowInstance.Value, ""));

        var controlPanel = await rFunc.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Failed);
        controlPanel.FatalWorkflowException.ShouldNotBeNull();
        
        await controlPanel.Postpone(new DateTime(1_000_000));

        await controlPanel.Refresh();
        controlPanel.Status.ShouldBe(Status.Postponed);
        controlPanel.PostponedUntil.ShouldNotBeNull();
        controlPanel.PostponedUntil.Value.Ticks.ShouldBe(1_000_000);
        
        var sf = await store.GetFunction(rFunc.MapToStoredId(functionId.Instance));
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Postponed);
        sf.Expires.ShouldBe(1_000_000);
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task FailingExistingActionFromControlPanelSucceeds();
    protected async Task FailingExistingActionFromControlPanelSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();

        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            Task (string _, Workflow workflow) => workflow.Delay(TimeSpan.FromMinutes(1))
        );
        
        await Should.ThrowAsync<Exception>(() => rAction.Invoke(flowInstance.Value, ""));

        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Postponed);
        controlPanel.PostponedUntil.ShouldNotBeNull();
        
        await controlPanel.Fail(new InvalidOperationException());

        await controlPanel.Refresh();
        controlPanel.Status.ShouldBe(Status.Failed);
        controlPanel.FatalWorkflowException.ShouldNotBeNull();

        var sf = await store.GetFunction(rAction.MapToStoredId(functionId.Instance));
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Failed);
        sf.Exception.ShouldNotBeNull();

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task FailingExistingFunctionFromControlPanelSucceeds();
    protected async Task FailingExistingFunctionFromControlPanelSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var rFunc = functionsRegistry.RegisterFunc<string, string>(
            flowType,
            async Task<string> (string _, Workflow workflow) =>
            {
                await workflow.Delay(TimeSpan.FromSeconds(10));
                return "Ok";
            }
        );
        
        await Should.ThrowAsync<Exception>(() => rFunc.Invoke(flowInstance.Value, ""));

        var controlPanel = await rFunc.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Postponed);
        controlPanel.PostponedUntil.ShouldNotBeNull();

        await controlPanel.Fail(new InvalidOperationException());

        await controlPanel.Refresh();
        controlPanel.Status.ShouldBe(Status.Failed);
        controlPanel.FatalWorkflowException.ShouldNotBeNull();
        
        var sf = await store.GetFunction(rFunc.MapToStoredId(functionId.Instance));
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Failed);
        sf.Exception.ShouldNotBeNull();
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task SucceedingExistingActionFromControlPanelSucceeds();
    protected async Task SucceedingExistingActionFromControlPanelSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            Task (string _) => throw new Exception("oh no")
        );
        
        await Should.ThrowAsync<Exception>(() => rAction.Invoke(flowInstance.Value, ""));

        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Failed);
        controlPanel.FatalWorkflowException.ShouldNotBeNull();

        await controlPanel.Succeed();

        await controlPanel.Refresh();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        
        var sf = await store.GetFunction(rAction.MapToStoredId(functionId.Instance));
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Succeeded);
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task SucceedingExistingParamlessFromControlPanelSucceeds();
    protected async Task SucceedingExistingParamlessFromControlPanelSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var paramlessRegistration = functionsRegistry.RegisterParamless(
            flowType,
            inner: Task () => throw new Exception("oh no")
        );
        
        await Should.ThrowAsync<Exception>(() => paramlessRegistration.Invoke(flowInstance.Value));

        var controlPanel = await paramlessRegistration.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Failed);
        controlPanel.FatalWorkflowException.ShouldNotBeNull();

        await controlPanel.Succeed();

        await controlPanel.Refresh();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        
        var sf = await store.GetFunction(paramlessRegistration.MapToStoredId(functionId.Instance));
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Succeeded);
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task SucceedingExistingFunctionFromControlPanelSucceeds();
    protected async Task SucceedingExistingFunctionFromControlPanelSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var rFunc = functionsRegistry.RegisterFunc<string, string>(
            flowType,
            Task<string> (_) => throw new Exception("oh no")
        );
        
        await Should.ThrowAsync<Exception>(() => rFunc.Invoke(flowInstance.Value, ""));

        var controlPanel = await rFunc.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Failed);
        controlPanel.FatalWorkflowException.ShouldNotBeNull();

        await controlPanel.Succeed("hello world");

        await controlPanel.Refresh();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        controlPanel.Result.ShouldBe("hello world");

        var storedId = rFunc.MapToStoredId(functionId.Instance);
        var sf = await store.GetFunction(storedId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Succeeded);
        var results = await store.GetResults([storedId]);
        var resultBytes = results[storedId];
        var result = (string)DefaultSerializer.Instance.Deserialize(resultBytes!, typeof(string));
        result.ShouldBe("hello world");
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task ReInvokingExistingFunctionFromControlPanelSucceeds();
    protected async Task ReinvokingExistingFunctionFromControlPanelSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = functionsRegistry.RegisterFunc(
            flowType,
            Task<string> (string param) => param.ToTask()
        );

        await rAction.Invoke(flowInstance.Value, param: "first");

        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        controlPanel.Result.ShouldBe("first");
        controlPanel.FatalWorkflowException.ShouldBeNull();

        controlPanel.Param = "second";
        var result = await controlPanel.Restart();
        result.ShouldBe("second");
        
        var sf = await store.GetFunction(rAction.MapToStoredId(functionId.Instance));
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Succeeded);
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task ScheduleReInvokingExistingActionFromControlPanelSucceeds();
    protected async Task ScheduleReInvokingExistingActionFromControlPanelSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            inner: Task (string param, Workflow workflow) => Task.CompletedTask 
        );

        await rAction.Invoke(flowInstance.Value, param: "first");

        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        controlPanel.FatalWorkflowException.ShouldBeNull();

        controlPanel.Param = "second";
        await controlPanel.SaveChanges();
        await controlPanel.Refresh();
        await controlPanel.ScheduleRestart();

        await BusyWait.Until(() => store.GetFunction(rAction.MapToStoredId(functionId.Instance)).SelectAsync(sf => sf?.Status == Status.Succeeded));
        await controlPanel.Refresh();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        
        var sf = await store.GetFunction(rAction.MapToStoredId(functionId.Instance));
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Succeeded);
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task WaitingForExistingFunctionFromControlPanelToCompleteSucceeds();
    protected async Task WaitingForExistingFunctionFromControlPanelToCompleteSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var flag = new SyncedFlag();
        var rFunc = functionsRegistry.RegisterFunc(
            flowType,
            async Task<string> (string param) =>
            {
                await flag.WaitForRaised();
                return param;
            });

        await rFunc.Schedule(flowInstance.Value, param: "param");

        var controlPanel = await rFunc.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Executing);

        var completionTask = controlPanel.WaitForCompletion();
        await Task.Delay(10);
        completionTask.IsCompleted.ShouldBeFalse();
        flag.Raise();

        await BusyWait.Until(() => completionTask.IsCompleted);

        var result = await completionTask;
        result.ShouldBe("param");
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task WaitingForExistingActionFromControlPanelToCompleteSucceeds();
    protected async Task WaitingForExistingActionFromControlPanelToCompleteSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var flag = new SyncedFlag();
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            Task(string param) => flag.WaitForRaised()
        );

        await rAction.Schedule(flowInstance.Value, param: "param");

        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Executing);

        var completionTask = controlPanel.WaitForCompletion();
        await Task.Delay(10);
        completionTask.IsCompleted.ShouldBeFalse();
        flag.Raise();

        await BusyWait.Until(() => completionTask.IsCompleted);

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task ReInvokeRFuncSucceedsAfterSuccessfullySavingParamAndState();
    protected async Task ReInvokeRFuncSucceedsAfterSuccessfullySavingParamAndState(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rAction = functionsRegistry.RegisterFunc(
            flowType,
            Task<string> (string param) => param.ToTask()
        );

        await rAction.Invoke(flowInstance.Value, param: "param");

        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        await controlPanel.SaveChanges();
        await controlPanel.Restart().ShouldBeAsync("param");
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task ReInvokeRActionSucceedsAfterSuccessfullySavingParamAndState();
    protected async Task ReInvokeRActionSucceedsAfterSuccessfullySavingParamAndState(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rAction = functionsRegistry.RegisterAction(
            flowType,
            Task (string _) => Task.CompletedTask
        );

        await rAction.Invoke(flowInstance.Value, param: "param");

        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        await controlPanel.SaveChanges();
        await controlPanel.Restart();
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task ControlPanelsExistingMessagesContainsPreviouslyAddedMessages();
    protected async Task ControlPanelsExistingMessagesContainsPreviouslyAddedMessages(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            async Task(string param, Workflow workflow) =>
            {
                await workflow.Messages.AppendMessage(param);
            }
        );

        await rAction.Invoke(flowInstance.Value, param: "param");

        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        var existingMessages = controlPanel.Messages;
        var messages = await existingMessages.AsObjects;
        messages.Count.ShouldBe(1);
        messages[0].ShouldBe("param");
        await existingMessages.Replace(0, "hello");

        await controlPanel.Refresh();
        var receivedMessages = await controlPanel
            .Messages
            .MessagesWithIdempotencyKeys;
        
        receivedMessages.Count.ShouldBe(1);
        receivedMessages.Single().Message.ShouldBe("hello");
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task ExistingMessagesCanBeReplacedUsingControlPanel();
    protected async Task ExistingMessagesCanBeReplacedUsingControlPanel(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var first = true;
        var invocationCount = new SyncedCounter();
        var syncedList = new SyncedList<string>();
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            async Task(string param, Workflow workflow) =>
            {
                var messages = workflow.Messages;
                if (first)
                {
                    invocationCount.Increment();
                    first = false;
                    await messages.AppendMessage("hello world", idempotencyKey: "1");
                    await messages.AppendMessage("hello universe", idempotencyKey: "2");
                }
                else
                {
                    var existing = await messages.Select(e => e.ToString()!).Take(2).ToList();
                    syncedList.AddRange(existing);
                }
            }
        );

        await rAction.Invoke(flowInstance.Value, param: "param");

        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        var existingMessages = controlPanel.Messages;
        await existingMessages.Count.ShouldBeAsync(2);
        await existingMessages.Clear();

        await existingMessages.Append("hello to you", "1");
        await existingMessages.Append("hello from me", "2");
        
        await controlPanel.SaveChanges();
        await controlPanel.Refresh();
        await controlPanel.Restart();
        
        await controlPanel.Messages.Count.ShouldBeAsync(2);
        
        syncedList.ShouldNotBeNull();
        if (syncedList.Count != 2)
            throw new Exception(
                $"Excepted only 2 messages (invocation count: {invocationCount.Current}) - there was: " + string.Join(", ", syncedList.Select(e => "'" + e.ToJson() + "'"))
            );
        
        syncedList.Count.ShouldBe(2);
        syncedList[0].ShouldBe("hello to you");
        syncedList[1].ShouldBe("hello from me");

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task ExistingMessagesAreNotAffectedByControlPanelSaveChangesInvocation();
    protected async Task ExistingMessagesAreNotAffectedByControlPanelSaveChangesInvocation(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var first = true;
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            async Task(string param, Workflow workflow) =>
            {
                var messages = workflow.Messages;
                if (first)
                {
                    first = false;
                    await messages.AppendMessage("hello world", idempotencyKey: "1");
                    await messages.AppendMessage("hello universe", idempotencyKey: "2");
                }
            }
        );

        await rAction.Invoke(flowInstance.Value, param: "param");

        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        controlPanel.Param = "test";
        await controlPanel.SaveChanges();
        await controlPanel.Refresh();

        var messages = await controlPanel.Messages.MessagesWithIdempotencyKeys;
        messages.Count.ShouldBe(2);
        messages[0].Message.ShouldBe("hello world");
        messages[0].IdempotencyKey.ShouldBe("1");
        messages[1].Message.ShouldBe("hello universe");
        messages[1].IdempotencyKey.ShouldBe("2");
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task ConcurrentModificationOfExistingMessagesCausesExceptionOnSaveChanges();
    protected async Task ConcurrentModificationOfExistingMessagesCausesExceptionOnSaveChanges(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            Task(string param, Workflow workflow) => Task.Delay(1)
        );

        await rAction.Invoke(flowInstance.Value, param: "param");
        await store.MessageStore.AppendMessage(
            rAction.MapToStoredId(functionId.Instance),
            new StoredMessage("hello world".ToJson().ToUtf8Bytes(), typeof(string).SimpleQualifiedName().ToUtf8Bytes(), Position: 0)
        );

        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        var existingMessages = controlPanel.Messages;
        await existingMessages.Count.ShouldBeAsync(1);

        await store.MessageStore.AppendMessage(
            rAction.MapToStoredId(functionId.Instance),
            new StoredMessage("hello universe".ToJson().ToUtf8Bytes(), typeof(string).SimpleQualifiedName().ToUtf8Bytes(), Position: 0)
        );
        
        await existingMessages.Clear();
        await existingMessages.Append("hej verden");
        await existingMessages.Append("hej univers");
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task ConcurrentModificationOfExistingMessagesDoesNotCauseExceptionOnSaveChangesWhenMessagesAreNotReplaced();
    protected async Task ConcurrentModificationOfExistingMessagesDoesNotCauseExceptionOnSaveChangesWhenMessagesAreNotReplaced(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            Task(string param, Workflow workflow) => Task.Delay(1)
        );

        await rAction.Invoke(flowInstance.Value, param: "param");
        await store.MessageStore.AppendMessage(
            rAction.MapToStoredId(functionId.Instance),
            new StoredMessage("hello world".ToJson().ToUtf8Bytes(), typeof(string).SimpleQualifiedName().ToUtf8Bytes(), Position: 0)
        );

        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();

        await store.MessageStore.AppendMessage(
            rAction.MapToStoredId(functionId.Instance),
            new StoredMessage("hello universe".ToJson().ToUtf8Bytes(), typeof(string).SimpleQualifiedName().ToUtf8Bytes(), Position: 0)
        );

        controlPanel.Param = "PARAM";
        await controlPanel.SaveChanges();
        var param = controlPanel.Param;
        await controlPanel.Refresh();
        controlPanel.Param.ShouldBe(param);

        var messages = await controlPanel.Messages.AsObjects;
        messages.Count.ShouldBe(2);
        messages[0].ShouldBe("hello world");
        messages[1].ShouldBe("hello universe");
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task ConcurrentModificationOfExistingMessagesCausesExceptionOnSave();
    protected async Task ConcurrentModificationOfExistingMessagesCausesExceptionOnSave(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            Task(string param, Workflow workflow) => Task.Delay(1)
        );

        await rAction.Invoke(flowInstance.Value, param: "param");
        await store.MessageStore.AppendMessage(
            rAction.MapToStoredId(functionId.Instance),
            new StoredMessage("hello world".ToJson().ToUtf8Bytes(), typeof(string).SimpleQualifiedName().ToUtf8Bytes(), Position: 0)
        );

        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        var existingMessages = controlPanel.Messages;
        await existingMessages.Count.ShouldBeAsync(1);

        await store.MessageStore.AppendMessage(
            rAction.MapToStoredId(functionId.Instance),
            new StoredMessage("hello universe".ToJson().ToUtf8Bytes(), typeof(string).SimpleQualifiedName().ToUtf8Bytes(), Position: 0)
        );
        
        await existingMessages.Clear();
        await existingMessages.Append("hej verden");
        await existingMessages.Append("hej univers");
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task ConcurrentModificationOfExistingMessagesDoesNotCauseExceptionOnSucceedWhenMessagesAreNotReplaced();
    protected async Task ConcurrentModificationOfExistingMessagesDoesNotCauseExceptionOnSucceedWhenMessagesAreNotReplaced(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            Task(string param, Workflow workflow) => Task.Delay(1)
        );

        await rAction.Invoke(flowInstance.Value, param: "param");
        await store.MessageStore.AppendMessage(
            rAction.MapToStoredId(functionId.Instance),
            new StoredMessage("hello world".ToJson().ToUtf8Bytes(), typeof(string).SimpleQualifiedName().ToUtf8Bytes(), Position: 0)
        );

        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();

        await store.MessageStore.AppendMessage(
            rAction.MapToStoredId(functionId.Instance),
            new StoredMessage("hello universe".ToJson().ToUtf8Bytes(), typeof(string).SimpleQualifiedName().ToUtf8Bytes(), Position: 0)
        );

        controlPanel.Param = "PARAM";
        await controlPanel.Succeed();
        var param = controlPanel.Param;
        await controlPanel.Refresh();
        controlPanel.Param.ShouldBe(param);

        var messages = await controlPanel.Messages.AsObjects;
        messages.Count.ShouldBe(2);
        messages[0].ShouldBe("hello world");
        messages[1].ShouldBe("hello universe");
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task ExistingMessagesCanBeReplaced();
    protected async Task ExistingMessagesCanBeReplaced(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            Task(string param, Workflow workflow) => Task.CompletedTask
        );

        await rAction.Invoke(flowInstance.Value, param: "param");
        await rAction.MessageWriters
            .For(flowInstance.Value.ToFlowInstance())
            .AppendMessage("hello world", idempotencyKey: "first");
            
        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        var existingMessages = controlPanel.Messages;
        var (message, idempotencyKey) = (await existingMessages.MessagesWithIdempotencyKeys).Single();
        message.ShouldBe("hello world");
        idempotencyKey.ShouldBe("first");

        await existingMessages.Clear();
        await existingMessages.Append("hello universe", idempotencyKey: "second");

        await controlPanel.Refresh();

        existingMessages = controlPanel.Messages;
        (message, idempotencyKey) = (await existingMessages.MessagesWithIdempotencyKeys).Single();
        message.ShouldBe("hello universe");
        idempotencyKey.ShouldBe("second");
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task ExistingEffectCanBeReplacedWithValue();
    protected async Task ExistingEffectCanBeReplacedWithValue(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var rFunc = functionsRegistry.RegisterFunc(
            flowType,
            Task<string> (string param, Workflow workflow) 
                => workflow.Effect.Capture(() => "EffectResult")
        );

        var result = await rFunc.Invoke(flowInstance.Value, param: "param");
        result.ShouldBe("EffectResult");
        
        var controlPanel = await rFunc.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        var effects = controlPanel.Effects;
        await effects.SetSucceeded(effectId: 0, result: "ReplacedResult");

        result = await controlPanel.Restart();
        result.ShouldBe("ReplacedResult");
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task EffectCanBeStarted();
    protected async Task EffectCanBeStarted(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var runEffect = false;
        
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            Task (string param, Workflow workflow) 
                => runEffect 
                    ? workflow.Effect.Capture(() => {}, ResiliencyLevel.AtMostOnce)
                    : Task.CompletedTask
        );

        await rAction.Invoke(flowInstance.Value, param: "param");
        
        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        var effects = controlPanel.Effects;
        await effects.SetStarted(effectId: 0);
        
        runEffect = true;
        await Should.ThrowAsync<Exception>(controlPanel.Restart());
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task EffectRawBytesResultCanFetched();
    protected async Task EffectRawBytesResultCanFetched(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rAction = functionsRegistry.RegisterParamless(
            flowType,
            Task (workflow) => workflow.Effect.Capture(() => 123)
        );

        await rAction.Invoke(flowInstance.Value);
        
        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        var effects = controlPanel.Effects;
        var bytes = await effects.GetResultBytes(0);
        bytes.ShouldNotBeNull();
        var result = bytes.ToStringFromUtf8Bytes();
        result.ShouldBe("123");
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task ExistingEffectCanBeReplaced();
    protected async Task ExistingEffectCanBeReplaced(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rFunc = functionsRegistry.RegisterAction(
            flowType,
            Task (string param, Workflow workflow) 
                => workflow.Effect.Capture(() => throw new InvalidOperationException("oh no"))
        );

        await Should.ThrowAsync<Exception>(rFunc.Invoke(flowInstance.Value, param: "param"));
        
        var controlPanel = await rFunc.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        var activities = controlPanel.Effects;
        await activities.SetSucceeded(effectId: 0);
        
        await controlPanel.Restart();
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task ExistingEffectCanBeRemoved();
    protected async Task ExistingEffectCanBeRemoved(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        var syncedCounter = new SyncedCounter();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rFunc = functionsRegistry.RegisterFunc(
            flowType,
            Task<string> (string param, Workflow workflow) =>
                workflow.Effect.Capture(() =>
                {
                    syncedCounter++;
                    return "EffectResult";
                })
        );

        var result = await rFunc.Invoke(flowInstance.Value, param: "param");
        result.ShouldBe("EffectResult");
        syncedCounter.Current.ShouldBe(1);

        var controlPanel = await rFunc.ControlPanel(flowInstance.Value);
        controlPanel.ShouldNotBeNull();
        result = await controlPanel.Restart();
        result.ShouldBe("EffectResult");
        syncedCounter.Current.ShouldBe(1);
        
        await controlPanel.Refresh();
        var activities = controlPanel.Effects;
        await activities.Remove(0);

        await controlPanel.Restart();

        result = await rFunc.Invoke(flowInstance.Value, param: "param");
        result.ShouldBe("EffectResult");
        syncedCounter.Current.ShouldBe(2);

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task EffectsAreOnlyFetchedOnPropertyInvocation();
    protected async Task EffectsAreOnlyFetchedOnPropertyInvocation(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rAction = functionsRegistry.RegisterAction(
            flowType,
            (string _, Workflow _) => Task.CompletedTask
        );
        await rAction.Invoke(flowInstance.Value, param: "param");
        
        var controlPanel = await rAction.ControlPanel(flowInstance.Value);
        controlPanel.ShouldNotBeNull();

        await controlPanel.Effects.AllIds;

        await store.EffectsStore.SetEffectResult(
            rAction.MapToStoredId(functionId.Instance),
            new StoredEffect(
                "SomeId".GetHashCode().ToEffectId(),
                WorkStatus.Completed,
                Result: "SomeResult".ToJson().ToUtf8Bytes(),
                StoredException: null,
                Alias: null
            ).ToStoredChange(rAction.MapToStoredId(functionId.Instance), Insert),
            session: null
        );

        await controlPanel.Effects.HasValue("SomeId".GetHashCode()).ShouldBeFalseAsync();

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task EffectsAreCachedAfterInitialFetch();
    protected async Task EffectsAreCachedAfterInitialFetch(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rAction = functionsRegistry.RegisterAction(
            flowType,
            (string _, Workflow _) => Task.CompletedTask
        );
        await rAction.Invoke(flowInstance.Value, param: "param");
        
        var controlPanel = await rAction.ControlPanel(flowInstance.Value);
        controlPanel.ShouldNotBeNull();

        await controlPanel.Effects.AllIds;

        await store.EffectsStore.SetEffectResult(
            rAction.MapToStoredId(functionId.Instance),
            new StoredEffect(
                "SomeId".GetHashCode().ToEffectId(),
                WorkStatus.Completed,
                Result: "SomeResult".ToJson().ToUtf8Bytes(),
                StoredException: null,
                Alias: null
            ).ToStoredChange(rAction.MapToStoredId(functionId.Instance), Insert),
            session: null
        );

        await controlPanel.Effects.HasValue("SomeId".GetHashCode()).ShouldBeFalseAsync();

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task EffectsAreUpdatedAfterRefresh();
    protected async Task EffectsAreUpdatedAfterRefresh(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rAction = functionsRegistry.RegisterAction(
            flowType,
            (string _, Workflow _) => Task.CompletedTask
        );
        await rAction.Invoke(flowInstance.Value, param: "param");
        
        var firstControlPanel = await rAction.ControlPanel(flowInstance.Value);
        firstControlPanel.ShouldNotBeNull();
        
        var secondControlPanel = await rAction.ControlPanel(flowInstance.Value);
        secondControlPanel.ShouldNotBeNull();
        await secondControlPanel.Effects.HasValue("Id".GetHashCode()).ShouldBeAsync(false);

        await firstControlPanel.Effects.SetSucceeded("Id".GetHashCode(), "SomeResult");

        await secondControlPanel.Refresh();
        await secondControlPanel.Effects.GetValue<string>("Id".GetHashCode()).ShouldBeAsync("SomeResult");
        await secondControlPanel.Effects.GetStatus("Id".GetHashCode().ToEffectId()).ShouldBeAsync(WorkStatus.Completed);
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task ExistingEffectCanBeSetToFailed();
    protected async Task ExistingEffectCanBeSetToFailed(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        var syncedCounter = new SyncedCounter();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rFunc = functionsRegistry.RegisterFunc(
            flowType,
            Task<string> (string param, Workflow workflow) =>
                workflow.Effect.Capture(() =>
                {
                    syncedCounter++;
                    return "EffectResult";
                })
        );

        var result = await rFunc.Invoke(flowInstance.Value, param: "param");
        result.ShouldBe("EffectResult");
        syncedCounter.Current.ShouldBe(1);

        var controlPanel = await rFunc.ControlPanel(flowInstance.Value);
        controlPanel.ShouldNotBeNull();
        var effects = controlPanel.Effects;
        await effects.SetFailed(effectId: 0, new InvalidOperationException("oh no"));

        await Should.ThrowAsync<FatalWorkflowException>(() => 
            controlPanel.Restart()
        );

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task SaveChangesPersistsChangedResult();
    protected async Task SaveChangesPersistsChangedResult(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var rAction = functionsRegistry.RegisterFunc<string, string>(
            flowType,
            inner: param => param.ToTask()
        );

        await rAction.Invoke(flowInstance.Value, param: "param");

        {
            var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
            controlPanel.Result.ShouldBe("param");
            await controlPanel.Succeed("changed");
        }
        
        {
            var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
            controlPanel.Result.ShouldBe("changed");
        }
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task ExistingTimeoutCanBeUpdatedForAction();
    protected async Task ExistingTimeoutCanBeUpdatedForAction(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var actionRegistration = functionsRegistry.RegisterAction(
            flowType,
            Task (string param, Workflow workflow) =>
                workflow.Messages.FlowRegisteredTimeouts.RegisterTimeout(
                    "someTimeoutId".GetHashCode(),
                    expiresAt: new DateTime(2100, 1,1, 1,1,1, DateTimeKind.Utc),
                    publishMessage: true
                )
        );

        await actionRegistration.Invoke(flowInstance.Value, param: "param");

        var controlPanel = await actionRegistration.ControlPanel(flowInstance.Value);
        controlPanel.ShouldNotBeNull();
        var timeouts = controlPanel.RegisteredTimeouts;
        (await timeouts.All).Count.ShouldBe(1);
        await timeouts["someTimeoutId".GetHashCode()].ShouldBeAsync(new DateTime(2100, 1,1, 1,1,1, DateTimeKind.Utc));

        await timeouts.Upsert("someOtherTimeoutId".GetHashCode(), new DateTime(2101, 1, 1, 1, 1, 1, DateTimeKind.Utc));
        (await timeouts.All).Count.ShouldBe(2);
        await timeouts["someOtherTimeoutId".GetHashCode()].ShouldBeAsync(new DateTime(2101, 1,1, 1,1,1, DateTimeKind.Utc));

        await controlPanel.Refresh();

        (await timeouts.All).Count.ShouldBe(2);
        await timeouts["someTimeoutId".GetHashCode()].ShouldBeAsync(new DateTime(2100, 1,1, 1,1,1, DateTimeKind.Utc));
        await timeouts["someOtherTimeoutId".GetHashCode()].ShouldBeAsync(new DateTime(2101, 1,1, 1,1,1, DateTimeKind.Utc));

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task ExistingTimeoutCanBeUpdatedForFunc();
    protected async Task ExistingTimeoutCanBeUpdatedForFunc(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var funcRegistration = functionsRegistry.RegisterFunc(
            flowType,
            async Task<string> (string param, Workflow workflow) =>
            {
                await workflow.Messages.FlowRegisteredTimeouts.RegisterTimeout(
                    "someTimeoutId".GetHashCode(),
                    expiresAt: new DateTime(2100, 1, 1, 1, 1, 1, DateTimeKind.Utc),
                    publishMessage: true
                );

                return param;
            }
        );

        await funcRegistration.Invoke(flowInstance.Value, param: "param");

        var controlPanel = await funcRegistration.ControlPanel(flowInstance.Value);
        controlPanel.ShouldNotBeNull();
        var timeouts = controlPanel.RegisteredTimeouts;
        (await timeouts.All).Count.ShouldBe(1);
        await timeouts["someTimeoutId".GetHashCode()].ShouldBeAsync(new DateTime(2100, 1,1, 1,1,1, DateTimeKind.Utc));

        await timeouts.Upsert("someOtherTimeoutId".GetHashCode(), new DateTime(2101, 1, 1, 1, 1, 1, DateTimeKind.Utc));
        (await timeouts.All).Count.ShouldBe(2);
        await timeouts["someOtherTimeoutId".GetHashCode()].ShouldBeAsync(new DateTime(2101, 1,1, 1,1,1, DateTimeKind.Utc));

        await controlPanel.Refresh();

        (await timeouts.All).Count.ShouldBe(2);
        await timeouts["someTimeoutId".GetHashCode()].ShouldBeAsync(new DateTime(2100, 1,1, 1,1,1, DateTimeKind.Utc));
        await timeouts["someOtherTimeoutId".GetHashCode()].ShouldBeAsync(new DateTime(2101, 1,1, 1,1,1, DateTimeKind.Utc));

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task CorrelationsCanBeChanged();
    protected async Task CorrelationsCanBeChanged(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var registration = functionsRegistry.RegisterParamless(
            flowType,
            async workflow =>
            {
                await workflow.Correlations.Register("SomeCorrelation");
                
            }
        );

        await registration.Invoke(flowInstance.Value);

        var controlPanel = await registration.ControlPanel(flowInstance.Value);
        controlPanel.ShouldNotBeNull();
       
        await controlPanel.Correlations.Contains("SomeCorrelation").ShouldBeTrueAsync();
        await controlPanel.Correlations.Remove("SomeCorrelation");
        await controlPanel.Correlations.Register("SomeNewCorrelation");

        controlPanel = await registration.ControlPanel(flowInstance.Value);
        controlPanel.ShouldNotBeNull();
        await controlPanel.Correlations.Contains("SomeCorrelation").ShouldBeFalseAsync();
        await controlPanel.Correlations.Contains("SomeNewCorrelation").ShouldBeTrueAsync();
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task DeleteRemovesFunctionFromAllStores();
    protected async Task DeleteRemovesFunctionFromAllStores(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var registration = functionsRegistry.RegisterParamless(
            flowType,
            inner: () => Task.CompletedTask
        );

        await registration.Invoke(flowInstance.Value);

        var controlPanel = await registration.ControlPanel(flowInstance.Value);
        controlPanel.ShouldNotBeNull();

        await controlPanel.Correlations.Register("SomeCorrelation");
        await controlPanel.Effects.SetSucceeded("SomeEffect".GetHashCode());
        await controlPanel.Messages.Append("Some Message");
        await controlPanel.RegisteredTimeouts.Upsert("SomeTimeout".GetHashCode(), expiresAt: DateTime.UtcNow.Add(TimeSpan.FromDays(1)));
        
        await controlPanel.Delete();

        var storedId = registration.MapToStoredId(functionId.Instance);
        await store.GetFunction(storedId).ShouldBeNullAsync();
        
        await store.MessageStore.GetMessages(storedId, skip: 0)
            .SelectAsync(msgs => msgs.Count == 0)
            .ShouldBeTrueAsync();

        await store.CorrelationStore.GetCorrelations(storedId)
            .SelectAsync(c => c.Any())
            .ShouldBeFalseAsync();

        await store.EffectsStore
            .GetEffectResults(storedId)
            .SelectAsync(e => e.Any())
            .ShouldBeFalseAsync();
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task ClearFailedEffectsRemovesFailedEffectBeforeRestart();
    protected async Task ClearFailedEffectsRemovesFailedEffectBeforeRestart(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var retryPolicy = RetryPolicy.CreateConstantDelay(
            interval: TimeSpan.FromMilliseconds(10),
            maximumAttempts: 1,
            suspendThreshold: TimeSpan.FromMinutes(5)
        );
        var shouldFail = true;
        var registration = functionsRegistry.RegisterParamless(
            flowType,
            inner: async workflow =>
            {
                await workflow.Effect.Capture(() =>
                {
                    if (shouldFail)
                        throw new TimeoutException("Timeout!");
                }, retryPolicy);
            } 
        );

        try
        {
            var timeoutEvent = new TimeoutEvent(EffectId.CreateWithRootContext("SomeTimeout".GetHashCode()), DateTime.UtcNow)
                .ToMessageAndIdempotencyKey();

            await registration.Invoke(
                flowInstance,
                InitialState.CreateWithMessagesOnly([timeoutEvent])
            );
        }
        catch (FatalWorkflowException exception)
        {
            exception.ErrorType.ShouldBe(typeof(TimeoutException));
        }
        
        var controlPanel = await registration.ControlPanel(flowInstance.Value);
        controlPanel.ShouldNotBeNull();
        
        try
        {
            await controlPanel.Restart();
        }
        catch (FatalWorkflowException exception)
        {
            exception.ErrorType.ShouldBe(typeof(TimeoutException));
        }

        await controlPanel.Effects.AllIds.SelectAsync(ids => ids.Any()).ShouldBeTrueAsync();
        await controlPanel.Messages.AsObjects.SelectAsync(ids => ids.Any()).ShouldBeTrueAsync();
        
        await controlPanel.ClearFailures();
        await controlPanel.Effects.AllIds.SelectAsync(ids => ids.Any()).ShouldBeFalseAsync();
        await controlPanel.Messages.AsObjects.SelectAsync(msgs => msgs.Any(msg => msg is not NoOp)).ShouldBeFalseAsync();
        
        shouldFail = false;
        await controlPanel.Restart();
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
}