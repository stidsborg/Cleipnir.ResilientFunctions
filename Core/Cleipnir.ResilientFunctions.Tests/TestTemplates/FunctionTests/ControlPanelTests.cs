using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
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
        ActionRegistration<string> rAction = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rAction = r.RegisterAction(
                flowType,
                async (string _, Workflow workflow) =>
                {
                    await workflow.Effect.CreateOrGet("alias", 123);
                    await workflow.AppendMessage("Message");
                }
            );
        });

        await rAction.Run(flowInstance.Value, "");

        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        await controlPanel.Delete();
        
        await Should.ThrowAsync<UnexpectedStateException>(controlPanel.Refresh());

        var storedId = rAction.MapToStoredId(functionId.Instance);
        await store.GetFunction(storedId).ShouldBeNullAsync();

        await store
            .MessageStore
            .GetMessages(storedId)
            .SelectAsync(messages => messages.Count)
            .ShouldBeAsync(0);

        await store
            
            .GetEffectResults(storedId)
            .SelectAsync(effects => effects.Count())
            .ShouldBeAsync(0);

        await store
            
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
        
        FuncRegistration<string, string> rFunc = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rFunc = r.RegisterFunc(
                flowType,
                async Task<string>(string _, Workflow workflow) =>
                {
                    await workflow.Effect.CreateOrGet("alias", 123);
                    await workflow.AppendMessage("Message");
                    return "hello";
                });
        });

        await rFunc.Run(flowInstance.Value, "");

        var controlPanel = await rFunc.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        await controlPanel.Delete();

        await Should.ThrowAsync<UnexpectedStateException>(controlPanel.Refresh());

        var storedId = rFunc.MapToStoredId(functionId.Instance);
        await store.GetFunction(storedId).ShouldBeNullAsync();
        
        await store
            .MessageStore
            .GetMessages(storedId)
            .SelectAsync(messages => messages.Count)
            .ShouldBeAsync(0);

        await store
            
            .GetEffectResults(storedId)
            .SelectAsync(effects => effects.Count())
            .ShouldBeAsync(0);

        await store
            
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
        
        ActionRegistration<string> rAction = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rAction = r.RegisterAction(
                flowType,
                Task (string _) => throw new InvalidOperationException("oh no")
            );
        });

        await Should.ThrowAsync<Exception>(() => rAction.Run(flowInstance.Value, ""));

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

        var fwe = (FatalWorkflowException) unhandledExceptionCatcher.ThrownExceptions.Single().InnerException!;
        fwe.ErrorType.ShouldBe(typeof(InvalidOperationException));
    }

    public abstract Task PostponingExistingFunctionFromControlPanelSucceeds();
    protected async Task PostponingExistingFunctionFromControlPanelSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        
        FuncRegistration<string, string> rFunc = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rFunc = r.RegisterFunc<string, string>(
                flowType,
                Task<string> (_) => throw new InvalidOperationException("oh no")
            );
        });

        await Should.ThrowAsync<Exception>(() => rFunc.Run(flowInstance.Value, ""));

        var controlPanel = await rFunc.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Failed);
        controlPanel.FatalWorkflowException.ShouldNotBeNull();

        var postponeUntil = DateTime.UtcNow.AddDays(1);
        await controlPanel.Postpone(postponeUntil);

        await controlPanel.Refresh();
        controlPanel.Status.ShouldBe(Status.Postponed);
        controlPanel.PostponedUntil.ShouldNotBeNull();
        controlPanel.PostponedUntil.Value.Ticks.ShouldBe(postponeUntil.Ticks);

        var sf = await store.GetFunction(rFunc.MapToStoredId(functionId.Instance));
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Postponed);
        sf.Expires.ShouldBe(postponeUntil.Ticks);

        var fwe = (FatalWorkflowException) unhandledExceptionCatcher.ThrownExceptions.Single().InnerException!;
        fwe.ErrorType.ShouldBe(typeof(InvalidOperationException));
    }

    public abstract Task FailingExistingActionFromControlPanelSucceeds();
    protected async Task FailingExistingActionFromControlPanelSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();

        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        
        ActionRegistration<string> rAction = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rAction = r.RegisterAction(
                flowType,
                Task (string _, Workflow workflow) => workflow.Delay(TimeSpan.FromMinutes(1))
            );
        });
        
        await Should.ThrowAsync<Exception>(() => rAction.Run(flowInstance.Value, ""));

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
        
        FuncRegistration<string, string> rFunc = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rFunc = r.RegisterFunc<string, string>(
                flowType,
                async Task<string> (string _, Workflow workflow) =>
                {
                    await workflow.Delay(TimeSpan.FromSeconds(10));
                    return "Ok";
                }
            );
        });
        
        await Should.ThrowAsync<Exception>(() => rFunc.Run(flowInstance.Value, ""));

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
        
        ActionRegistration<string> rAction = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rAction = r.RegisterAction(
                flowType,
                Task (string _) => throw new InvalidOperationException("oh no")
            );
        });

        await Should.ThrowAsync<Exception>(() => rAction.Run(flowInstance.Value, ""));

        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Failed);
        controlPanel.FatalWorkflowException.ShouldNotBeNull();

        await controlPanel.Succeed();

        await controlPanel.Refresh();
        controlPanel.Status.ShouldBe(Status.Succeeded);

        var sf = await store.GetFunction(rAction.MapToStoredId(functionId.Instance));
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Succeeded);

        var fwe = (FatalWorkflowException) unhandledExceptionCatcher.ThrownExceptions.Single().InnerException!;
        fwe.ErrorType.ShouldBe(typeof(InvalidOperationException));
    }

    public abstract Task SucceedingExistingParamlessFromControlPanelSucceeds();
    protected async Task SucceedingExistingParamlessFromControlPanelSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        
        ParamlessRegistration paramlessRegistration = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            paramlessRegistration = r.RegisterParamless(
                flowType,
                inner: Task () => throw new InvalidOperationException("oh no")
            );
        });

        await Should.ThrowAsync<Exception>(() => paramlessRegistration.Run(flowInstance.Value));

        var controlPanel = await paramlessRegistration.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Failed);
        controlPanel.FatalWorkflowException.ShouldNotBeNull();

        await controlPanel.Succeed();

        await controlPanel.Refresh();
        controlPanel.Status.ShouldBe(Status.Succeeded);

        var sf = await store.GetFunction(paramlessRegistration.MapToStoredId(functionId.Instance));
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Succeeded);

        var fwe = (FatalWorkflowException) unhandledExceptionCatcher.ThrownExceptions.Single().InnerException!;
        fwe.ErrorType.ShouldBe(typeof(InvalidOperationException));
    }

    public abstract Task SucceedingExistingFunctionFromControlPanelSucceeds();
    protected async Task SucceedingExistingFunctionFromControlPanelSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        
        FuncRegistration<string, string> rFunc = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rFunc = r.RegisterFunc<string, string>(
                flowType,
                Task<string> (_) => throw new InvalidOperationException("oh no")
            );
        });

        await Should.ThrowAsync<Exception>(() => rFunc.Run(flowInstance.Value, ""));

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

        var fwe = (FatalWorkflowException) unhandledExceptionCatcher.ThrownExceptions.Single().InnerException!;
        fwe.ErrorType.ShouldBe(typeof(InvalidOperationException));
    }

    public abstract Task ReInvokingExistingFunctionFromControlPanelSucceeds();
    protected async Task ReinvokingExistingFunctionFromControlPanelSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        FuncRegistration<string, string> rAction = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rAction = r.RegisterFunc(
                flowType,
                Task<string> (string param) => param.ToTask()
            );
        });

        await rAction.Run(flowInstance.Value, param: "first");

        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        controlPanel.Result.ShouldBe("first");
        controlPanel.FatalWorkflowException.ShouldBeNull();

        controlPanel.Param = "second";
        var result = await controlPanel.ScheduleRestart().Completion();
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
        ActionRegistration<string> rAction = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rAction = r.RegisterAction(
                flowType,
                inner: Task (string param, Workflow workflow) => Task.CompletedTask
            );
        });

        await rAction.Run(flowInstance.Value, param: "first");

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
        var flag = new SyncedFlag();
        FuncRegistration<string, string> rFunc = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rFunc = r.RegisterFunc(
                flowType,
                async Task<string> (string param) =>
                {
                    await flag.WaitForRaised();
                    return param;
                });
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
        var flag = new SyncedFlag();
        ActionRegistration<string> rAction = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rAction = r.RegisterAction(
                flowType,
                Task(string param) => flag.WaitForRaised()
            );
        });

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
        FuncRegistration<string, string> rAction = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rAction = r.RegisterFunc(
                flowType,
                Task<string> (string param) => param.ToTask()
            );
        });

        await rAction.Run(flowInstance.Value, param: "param");

        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        await controlPanel.SaveChanges();
        await controlPanel.ScheduleRestart().Completion().ShouldBeAsync("param");
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task ReInvokeRActionSucceedsAfterSuccessfullySavingParamAndState();
    protected async Task ReInvokeRActionSucceedsAfterSuccessfullySavingParamAndState(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        ActionRegistration<string> rAction = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rAction = r.RegisterAction(
                flowType,
                Task (string _) => Task.CompletedTask
            );
        });

        await rAction.Run(flowInstance.Value, param: "param");

        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        await controlPanel.SaveChanges();
        await controlPanel.ScheduleRestart().Completion();

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task ControlPanelsExistingMessagesContainsPreviouslyAddedMessages();
    protected async Task ControlPanelsExistingMessagesContainsPreviouslyAddedMessages(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        ActionRegistration<string> rAction = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rAction = r.RegisterAction(
                flowType,
                async Task (string param, Workflow workflow) =>
                {
                    await workflow.Message<string>();
                }
            );
        });

        await rAction.Schedule(flowInstance.Value, param: "param");

        // The flow awaits a string, so the int delivered from the outside is admitted and staged but never
        // matches - once the flow has suspended again the message sits durably in effect state, visible to the
        // control panel.
        await rAction.SendMessage(flowInstance, new NonStringMessage(42));
        await WaitUntilSuspendedWithMessageCount(rAction, flowInstance, messageCount: 1);

        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        var existingMessages = controlPanel.Messages;
        var messages = await existingMessages.AsObjects;
        messages.Count.ShouldBe(1);
        messages[0].ShouldBe(new NonStringMessage(42));
        await existingMessages.Clear();
        await existingMessages.Append("hello");

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
        var syncedList = new SyncedList<string>();
        ActionRegistration<string> rAction = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rAction = r.RegisterAction(
                flowType,
                async Task(string param, Workflow workflow) =>
                {
                    // Collect locally and publish at the end: a legal mid-flow suspend/replay must not leave a
                    // partial incarnation's messages behind in the asserted list.
                    var received = new List<string>();
                    for (var i = 0; i < 2; i++)
                    {
                        var msg = await workflow.Message<string>();
                        received.Add(msg);
                    }

                    syncedList.Clear();
                    syncedList.AddRange(received);
                }
            );
        });

        await rAction.Schedule(flowInstance.Value, param: "param");

        // The flow awaits strings, so the ints delivered from the outside are admitted and staged but never
        // delivered - the control panel replaces them in place with the strings the flow is waiting for, and the
        // restarted flow consumes those and runs to completion.
        await rAction.SendMessage(flowInstance, new NonStringMessage(1), idempotencyKey: "1");
        await rAction.SendMessage(flowInstance, new NonStringMessage(2), idempotencyKey: "2");
        await WaitUntilSuspendedWithMessageCount(rAction, flowInstance, messageCount: 2);

        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        var existingMessages = controlPanel.Messages;
        await existingMessages.Replace(0, "hello to you", "1");
        await existingMessages.Replace(1, "hello from me", "2");

        await controlPanel.ScheduleRestart().Completion();

        syncedList.ShouldNotBeNull();
        if (syncedList.Count != 2)
            throw new Exception(
                "Excepted only 2 messages - there was: " + string.Join(", ", syncedList.Select(e => "'" + e.ToJson() + "'"))
            );

        syncedList.Count.ShouldBe(2);
        syncedList[0].ShouldBe("hello to you");
        syncedList[1].ShouldBe("hello from me");

        await controlPanel.Refresh();
        controlPanel.Status.ShouldBe(Status.Succeeded);

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task AppendedMessageCanBeReplacedInPlace();
    protected async Task AppendedMessageCanBeReplacedInPlace(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();

        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        var first = true;
        var syncedList = new SyncedList<string>();
        ActionRegistration<string> rAction = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rAction = r.RegisterAction(
                flowType,
                async Task(string param, Workflow workflow) =>
                {
                    if (first)
                    {
                        first = false;
                        return;
                    }

                    var received = new List<string>();
                    for (var i = 0; i < 2; i++)
                        received.Add(await workflow.Message<string>());

                    syncedList.Clear();
                    syncedList.AddRange(received);
                }
            );
        });

        await rAction.Run(flowInstance.Value, param: "param");

        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        var existingMessages = controlPanel.Messages;
        await existingMessages.Append("hello to you", "1");
        await existingMessages.Append("hello from me", "2");
        await existingMessages.Replace(1, "hello universe", "3");

        var messages = await existingMessages.MessagesWithIdempotencyKeys;
        messages.Count.ShouldBe(2);
        messages[0].Message.ShouldBe("hello to you");
        messages[0].IdempotencyKey.ShouldBe("1");
        messages[1].Message.ShouldBe("hello universe");
        messages[1].IdempotencyKey.ShouldBe("3");

        await controlPanel.ScheduleRestart().Completion();

        syncedList.Count.ShouldBe(2);
        syncedList[0].ShouldBe("hello to you");
        syncedList[1].ShouldBe("hello universe");

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task ExistingMessagesAreNotAffectedByControlPanelSaveChangesInvocation();
    protected async Task ExistingMessagesAreNotAffectedByControlPanelSaveChangesInvocation(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        ActionRegistration<string> rAction = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rAction = r.RegisterAction(
                flowType,
                async Task (string param, Workflow workflow) =>
                {
                    await workflow.Message<string>();
                }
            );
        });

        await rAction.Schedule(flowInstance.Value, param: "param");

        // The flow awaits a string, so the ints delivered from the outside are admitted and staged but never
        // match - they sit durably in effect state once the flow has suspended again.
        await rAction.SendMessage(flowInstance, new NonStringMessage(42), idempotencyKey: "1");
        await rAction.SendMessage(flowInstance, new NonStringMessage(43), idempotencyKey: "2");
        await WaitUntilSuspendedWithMessageCount(rAction, flowInstance, messageCount: 2);

        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        controlPanel.Param = "test";
        await controlPanel.SaveChanges();
        await controlPanel.Refresh();

        var messages = await controlPanel.Messages.MessagesWithIdempotencyKeys;
        messages.Count.ShouldBe(2);
        messages[0].Message.ShouldBe(new NonStringMessage(42));
        messages[0].IdempotencyKey.ShouldBe("1");
        messages[1].Message.ShouldBe(new NonStringMessage(43));
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
        ActionRegistration<string> rAction = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rAction = r.RegisterAction(
                flowType,
                Task(string param, Workflow workflow) => Task.Delay(1)
            );
        });

        await rAction.Run(flowInstance.Value, param: "param");

        var concurrentControlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        await concurrentControlPanel.Messages.Append("hello world");

        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        var existingMessages = controlPanel.Messages;
        await existingMessages.Count.ShouldBeAsync(1);

        await concurrentControlPanel.Messages.Append("hello universe");

        await existingMessages.Clear();
        await existingMessages.Append("hej verden");
        await existingMessages.Append("hej univers");
        await existingMessages.Count.ShouldBeAsync(2);

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task ConcurrentModificationOfExistingMessagesDoesNotCauseExceptionOnSaveChangesWhenMessagesAreNotReplaced();
    protected async Task ConcurrentModificationOfExistingMessagesDoesNotCauseExceptionOnSaveChangesWhenMessagesAreNotReplaced(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        ActionRegistration<string> rAction = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rAction = r.RegisterAction(
                flowType,
                async Task (string param, Workflow workflow) =>
                {
                    await workflow.Message<string>();
                }
            );
        });

        await rAction.Schedule(flowInstance.Value, param: "param");

        // The first int delivered from the outside is admitted and staged but never matches the string
        // subscription - the control panel is created against that settled, suspended state.
        await rAction.SendMessage(flowInstance, new NonStringMessage(42));
        await WaitUntilSuspendedWithMessageCount(rAction, flowInstance, messageCount: 1);

        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();

        // The second arrival restarts and re-suspends the flow behind the control panel's back.
        await rAction.SendMessage(flowInstance, new NonStringMessage(43));
        await WaitUntilSuspendedWithMessageCount(rAction, flowInstance, messageCount: 2);

        controlPanel.Param = "PARAM";
        await controlPanel.SaveChanges();
        var param = controlPanel.Param;
        await controlPanel.Refresh();
        controlPanel.Param.ShouldBe(param);

        var messages = await controlPanel.Messages.AsObjects;
        messages.Count.ShouldBe(2);
        messages[0].ShouldBe(new NonStringMessage(42));
        messages[1].ShouldBe(new NonStringMessage(43));
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task ConcurrentModificationOfExistingMessagesCausesExceptionOnSave();
    protected async Task ConcurrentModificationOfExistingMessagesCausesExceptionOnSave(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        ActionRegistration<string> rAction = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rAction = r.RegisterAction(
                flowType,
                Task(string param, Workflow workflow) => Task.Delay(1)
            );
        });

        await rAction.Run(flowInstance.Value, param: "param");

        var concurrentControlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        await concurrentControlPanel.Messages.Append("hello world");

        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        var existingMessages = controlPanel.Messages;
        await existingMessages.Count.ShouldBeAsync(1);

        await concurrentControlPanel.Messages.Append("hello universe");

        await existingMessages.Clear();
        await existingMessages.Append("hej verden");
        await existingMessages.Append("hej univers");
        await existingMessages.Count.ShouldBeAsync(2);

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task ConcurrentModificationOfExistingMessagesDoesNotCauseExceptionOnSucceedWhenMessagesAreNotReplaced();
    protected async Task ConcurrentModificationOfExistingMessagesDoesNotCauseExceptionOnSucceedWhenMessagesAreNotReplaced(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        ActionRegistration<string> rAction = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rAction = r.RegisterAction(
                flowType,
                async Task (string param, Workflow workflow) =>
                {
                    await workflow.Message<string>();
                }
            );
        });

        await rAction.Schedule(flowInstance.Value, param: "param");

        // The first int delivered from the outside is admitted and staged but never matches the string
        // subscription - the control panel is created against that settled, suspended state.
        await rAction.SendMessage(flowInstance, new NonStringMessage(42));
        await WaitUntilSuspendedWithMessageCount(rAction, flowInstance, messageCount: 1);

        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();

        // The second arrival restarts and re-suspends the flow behind the control panel's back.
        await rAction.SendMessage(flowInstance, new NonStringMessage(43));
        await WaitUntilSuspendedWithMessageCount(rAction, flowInstance, messageCount: 2);

        controlPanel.Param = "PARAM";
        await controlPanel.Succeed();
        var param = controlPanel.Param;
        await controlPanel.Refresh();
        controlPanel.Param.ShouldBe(param);

        var messages = await controlPanel.Messages.AsObjects;
        messages.Count.ShouldBe(2);
        messages[0].ShouldBe(new NonStringMessage(42));
        messages[1].ShouldBe(new NonStringMessage(43));
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task ExistingMessagesCanBeReplaced();
    protected async Task ExistingMessagesCanBeReplaced(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        ActionRegistration<string> rAction = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rAction = r.RegisterAction(
                flowType,
                Task(string param, Workflow workflow) => Task.CompletedTask
            );
        });

        await rAction.Run(flowInstance.Value, param: "param");

        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        var existingMessages = controlPanel.Messages;
        await existingMessages.Append("hello world", idempotencyKey: "first");
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
        FuncRegistration<string, string> rFunc = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rFunc = r.RegisterFunc(
                flowType,
                Task<string> (string param, Workflow workflow)
                    => workflow.Effect.Capture(() => "EffectResult")
            );
        });

        var result = await rFunc.Run(flowInstance.Value, param: "param");
        result.ShouldBe("EffectResult");
        
        var controlPanel = await rFunc.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        var effects = controlPanel.Effects;
        await effects.SetSucceeded(effectId: 0, result: "ReplacedResult");

        result = await controlPanel.ScheduleRestart().Completion();
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
        var runEffect = false;
        ActionRegistration<string> rAction = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rAction = r.RegisterAction(
                flowType,
                Task (string param, Workflow workflow)
                    => runEffect
                        ? workflow.Effect.Capture(() => {}, ResiliencyLevel.AtMostOnce)
                        : Task.CompletedTask
            );
        });

        await rAction.Run(flowInstance.Value, param: "param");
        
        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        var effects = controlPanel.Effects;
        await effects.SetStarted(effectId: 0);
        
        runEffect = true;
        await controlPanel.ScheduleRestart();
        await Should.ThrowAsync<Exception>(() => controlPanel.WaitForCompletion());
    }
    
    public abstract Task EffectRawBytesResultCanFetched();
    protected async Task EffectRawBytesResultCanFetched(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        ParamlessRegistration rAction = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rAction = r.RegisterParamless(
                flowType,
                Task (workflow) => workflow.Effect.Capture(() => 123)
            );
        });

        await rAction.Run(flowInstance.Value);
        
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
        ActionRegistration<string> rFunc = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rFunc = r.RegisterAction(
                flowType,
                Task (string param, Workflow workflow)
                    => workflow.Effect.Capture(() => throw new InvalidOperationException("oh no"))
            );
        });

        await Should.ThrowAsync<Exception>(rFunc.Run(flowInstance.Value, param: "param"));
        
        var controlPanel = await rFunc.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        var activities = controlPanel.Effects;
        await activities.SetSucceeded(effectId: 0);
        
        await controlPanel.ScheduleRestart().Completion();

        var fwe = (FatalWorkflowException) unhandledExceptionCatcher.ThrownExceptions.Single().InnerException!;
        fwe.ErrorType.ShouldBe(typeof(InvalidOperationException));
    }

    public abstract Task ExistingEffectCanBeRemoved();
    protected async Task ExistingEffectCanBeRemoved(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        var syncedCounter = new SyncedCounter();
        FuncRegistration<string, string> rFunc = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rFunc = r.RegisterFunc(
                flowType,
                Task<string> (string param, Workflow workflow) =>
                    workflow.Effect.Capture(() =>
                    {
                        syncedCounter++;
                        return "EffectResult";
                    })
            );
        });

        var result = await rFunc.Run(flowInstance.Value, param: "param");
        result.ShouldBe("EffectResult");
        syncedCounter.Current.ShouldBe(1);

        var controlPanel = await rFunc.ControlPanel(flowInstance.Value);
        controlPanel.ShouldNotBeNull();
        result = await controlPanel.ScheduleRestart().Completion();
        result.ShouldBe("EffectResult");
        syncedCounter.Current.ShouldBe(1);

        await controlPanel.Refresh();
        var activities = controlPanel.Effects;
        await activities.Remove(0);

        await controlPanel.ScheduleRestart().Completion();

        result = await rFunc.Run(flowInstance.Value, param: "param");
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
        ActionRegistration<string> rAction = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rAction = r.RegisterAction(
                flowType,
                (string _, Workflow _) => Task.CompletedTask
            );
        });
        await rAction.Run(flowInstance.Value, param: "param");
        
        var controlPanel = await rAction.ControlPanel(flowInstance.Value);
        controlPanel.ShouldNotBeNull();

        await controlPanel.Effects.AllIds;

        await store.SetEffectResult(
            rAction.MapToStoredId(functionId.Instance),
            new StoredEffect(
                "SomeId".GetHashCode().ToEffectId(),
                WorkStatus.Completed,
                Result: "SomeResult".ToJson().ToUtf8Bytes(),
                StoredException: null,
                Alias: null
            ).ToStoredChange(rAction.MapToStoredId(functionId.Instance), Insert),
            owner: null, session: null
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
        ActionRegistration<string> rAction = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rAction = r.RegisterAction(
                flowType,
                (string _, Workflow _) => Task.CompletedTask
            );
        });
        await rAction.Run(flowInstance.Value, param: "param");
        
        var controlPanel = await rAction.ControlPanel(flowInstance.Value);
        controlPanel.ShouldNotBeNull();

        await controlPanel.Effects.AllIds;

        await store.SetEffectResult(
            rAction.MapToStoredId(functionId.Instance),
            new StoredEffect(
                "SomeId".GetHashCode().ToEffectId(),
                WorkStatus.Completed,
                Result: "SomeResult".ToJson().ToUtf8Bytes(),
                StoredException: null,
                Alias: null
            ).ToStoredChange(rAction.MapToStoredId(functionId.Instance), Insert),
            owner: null, session: null
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
        ActionRegistration<string> rAction = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rAction = r.RegisterAction(
                flowType,
                (string _, Workflow _) => Task.CompletedTask
            );
        });
        await rAction.Run(flowInstance.Value, param: "param");
        
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
        FuncRegistration<string, string> rFunc = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rFunc = r.RegisterFunc(
                flowType,
                Task<string> (string param, Workflow workflow) =>
                    workflow.Effect.Capture(() =>
                    {
                        syncedCounter++;
                        return "EffectResult";
                    })
            );
        });

        var result = await rFunc.Run(flowInstance.Value, param: "param");
        result.ShouldBe("EffectResult");
        syncedCounter.Current.ShouldBe(1);

        var controlPanel = await rFunc.ControlPanel(flowInstance.Value);
        controlPanel.ShouldNotBeNull();
        var effects = controlPanel.Effects;
        await effects.SetFailed(effectId: 0, new InvalidOperationException("oh no"));

        await controlPanel.ScheduleRestart();
        await Should.ThrowAsync<FatalWorkflowException>(() => controlPanel.WaitForCompletion());
    }

    public abstract Task SaveChangesPersistsChangedResult();
    protected async Task SaveChangesPersistsChangedResult(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        FuncRegistration<string, string> rAction = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            rAction = r.RegisterFunc<string, string>(
                flowType,
                inner: param => param.ToTask()
            );
        });

        await rAction.Run(flowInstance.Value, param: "param");

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

    public abstract Task DeleteRemovesFunctionFromAllStores();
    protected async Task DeleteRemovesFunctionFromAllStores(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();

        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        ParamlessRegistration registration = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            registration = r.RegisterParamless(
                flowType,
                inner: () => Task.CompletedTask
            );
        });

        await registration.Run(flowInstance.Value);

        var controlPanel = await registration.ControlPanel(flowInstance.Value);
        controlPanel.ShouldNotBeNull();

        await controlPanel.Effects.SetSucceeded("SomeEffect".GetHashCode());
        await controlPanel.Messages.Append("Some Message");

        await controlPanel.Delete();

        var storedId = registration.MapToStoredId(functionId.Instance);
        await store.GetFunction(storedId).ShouldBeNullAsync();

        await store.MessageStore.GetMessages(storedId)
            .SelectAsync(msgs => msgs.Count == 0)
            .ShouldBeTrueAsync();

        await store
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
        var retryPolicy = RetryPolicy.CreateConstantDelay(
            interval: TimeSpan.FromMilliseconds(10),
            maximumAttempts: 1,
            suspendThreshold: TimeSpan.FromMinutes(5)
        );
        var shouldFail = true;
        ParamlessRegistration registration = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(store, new Settings(unhandledExceptionCatcher.Catch), r =>
        {
            registration = r.RegisterParamless(
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
        });

        try
        {
            await registration.Run(flowInstance);
        }
        catch (FatalWorkflowException exception)
        {
            exception.ErrorType.ShouldBe(typeof(TimeoutException));
        }
        
        var controlPanel = await registration.ControlPanel(flowInstance.Value);
        controlPanel.ShouldNotBeNull();
        
        await controlPanel.ScheduleRestart();
        await Should.ThrowAsync<FatalWorkflowException>(() => controlPanel.WaitForCompletion());

        await controlPanel.Refresh();
        await controlPanel.Effects.AllIds.SelectAsync(ids => ids.Any()).ShouldBeTrueAsync();

        await controlPanel.ClearFailures();
        await controlPanel.Effects.AllIds.SelectAsync(ids => ids.Any()).ShouldBeFalseAsync();

        shouldFail = false;
        await controlPanel.ScheduleRestart().Completion();

        unhandledExceptionCatcher.ThrownExceptions.ShouldNotBeEmpty();
        foreach (var thrownException in unhandledExceptionCatcher.ThrownExceptions)
        {
            var fwe = (FatalWorkflowException) thrownException.InnerException!;
            fwe.ErrorType.ShouldBe(typeof(TimeoutException));
        }
    }

    // Reference type deliberately distinct from string: delivered to a flow awaiting Message<string> it is
    // admitted and staged but never matches, parking it durably in effect state for the control panel.
    private record NonStringMessage(int Value);

    // Waits until the flow has settled suspended with the expected number of staged messages. Order matters: the
    // count is read before the status, so an observed Suspended is the suspension AFTER the last admission - a
    // status read first could be the previous suspension's, with the flow still owned by the admitting restart,
    // making a control panel created afterwards capture the wrong owner.
    private static Task WaitUntilSuspendedWithMessageCount(ActionRegistration<string> registration, FlowInstance flowInstance, int messageCount)
        => BusyWait.Until(async () =>
        {
            var controlPanel = await registration.ControlPanel(flowInstance);
            if (await controlPanel!.Messages.Count != messageCount)
                return false;
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Suspended;
        });
}