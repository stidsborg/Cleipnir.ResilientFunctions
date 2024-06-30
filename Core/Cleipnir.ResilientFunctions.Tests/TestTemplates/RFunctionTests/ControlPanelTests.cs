using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class ControlPanelTests
{
    public abstract Task ExistingActionCanBeDeletedFromControlPanel();
    protected async Task ExistingActionCanBeDeletedFromControlPanel(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
            async (string _, Workflow workflow) =>
            {
                var (effect, messages, states) = workflow;
                await effect.CreateOrGet("Effect", 123);
                await messages.AppendMessage("Message");
                var state = states.CreateOrGet<State>();
                state.Value = "State";
                await workflow.Messages.TimeoutProvider.RegisterTimeout("Timeout", TimeSpan.FromDays(1));
                await state.Save();
            }
        );
        
        await rAction.Invoke(functionInstanceId.Value, "");

        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        await controlPanel.Delete();

        await Should.ThrowAsync<UnexpectedFunctionState>(controlPanel.Refresh());

        await store.GetFunction(functionId).ShouldBeNullAsync();

        await store
            .MessageStore
            .GetMessages(functionId, skip: 0)
            .SelectAsync(messages => messages.Count)
            .ShouldBeAsync(0);

        await store
            .EffectsStore
            .GetEffectResults(functionId)
            .SelectAsync(effects => effects.Count())
            .ShouldBeAsync(0);

        await store
            .StatesStore
            .GetStates(functionId)
            .SelectAsync(states => states.Count())
            .ShouldBeAsync(0);

        await store
            .TimeoutStore
            .GetTimeouts(functionId)
            .SelectAsync(timeouts => timeouts.Count())
            .ShouldBeAsync(0);
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ExistingFunctionCanBeDeletedFromControlPanel();
    protected async Task ExistingFunctionCanBeDeletedFromControlPanel(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var rFunc = functionsRegistry.RegisterFunc(
            functionTypeId,
            async Task<string>(string _, Workflow workflow) =>
            {
                var (effect, messages, states) = workflow;
                await effect.CreateOrGet("Effect", 123);
                await messages.AppendMessage("Message");
                var state = states.CreateOrGet<State>();
                state.Value = "State";
                await state.Save();
                await workflow.Messages.TimeoutProvider.RegisterTimeout("Timeout", TimeSpan.FromDays(1));
                return "hello";
            });
        
        await rFunc.Invoke(functionInstanceId.Value, "");

        var controlPanel = await rFunc.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        await controlPanel.Delete();

        await Should.ThrowAsync<UnexpectedFunctionState>(controlPanel.Refresh());

        await store.GetFunction(functionId).ShouldBeNullAsync();
        
        await store
            .MessageStore
            .GetMessages(functionId, skip: 0)
            .SelectAsync(messages => messages.Count)
            .ShouldBeAsync(0);

        await store
            .EffectsStore
            .GetEffectResults(functionId)
            .SelectAsync(effects => effects.Count())
            .ShouldBeAsync(0);

        await store
            .StatesStore
            .GetStates(functionId)
            .SelectAsync(states => states.Count())
            .ShouldBeAsync(0);
        
        await store
            .TimeoutStore
            .GetTimeouts(functionId)
            .SelectAsync(timeouts => timeouts.Count())
            .ShouldBeAsync(0);
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task PostponingExistingActionFromControlPanelSucceeds();
    protected async Task PostponingExistingActionFromControlPanelSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();

        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
            void (string _) => throw new Exception("oh no")
        );
        
        await Should.ThrowAsync<Exception>(() => rAction.Invoke(functionInstanceId.Value, ""));

        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Failed);
        controlPanel.PreviouslyThrownException.ShouldNotBeNull();
        
        await controlPanel.Postpone(new DateTime(1_000_000));

        await controlPanel.Refresh();
        controlPanel.Status.ShouldBe(Status.Postponed);
        controlPanel.PostponedUntil.ShouldNotBeNull();
        controlPanel.PostponedUntil.Value.Ticks.ShouldBe(1_000_000);
        
        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Postponed);
        sf.PostponedUntil.ShouldNotBeNull();
        sf.PostponedUntil.Value.ShouldBe(1_000_000);
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task PostponingExistingFunctionFromControlPanelSucceeds();
    protected async Task PostponingExistingFunctionFromControlPanelSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var rFunc = functionsRegistry.RegisterFunc<string, string>(
            functionTypeId,
            string (_) => throw new Exception("oh no")
        );
        
        await Should.ThrowAsync<Exception>(() => rFunc.Invoke(functionInstanceId.Value, ""));

        var controlPanel = await rFunc.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Failed);
        controlPanel.PreviouslyThrownException.ShouldNotBeNull();
        
        await controlPanel.Postpone(new DateTime(1_000_000));

        await controlPanel.Refresh();
        controlPanel.Status.ShouldBe(Status.Postponed);
        controlPanel.PostponedUntil.ShouldNotBeNull();
        controlPanel.PostponedUntil.Value.Ticks.ShouldBe(1_000_000);
        
        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Postponed);
        sf.PostponedUntil.ShouldNotBeNull();
        sf.PostponedUntil.Value.ShouldBe(1_000_000);
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task FailingExistingActionFromControlPanelSucceeds();
    protected async Task FailingExistingActionFromControlPanelSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();

        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
            void (string _) => throw new PostponeInvocationException(TimeSpan.FromMinutes(1))
        );
        
        await Should.ThrowAsync<Exception>(() => rAction.Invoke(functionInstanceId.Value, ""));

        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Postponed);
        controlPanel.PostponedUntil.ShouldNotBeNull();
        
        await controlPanel.Fail(new InvalidOperationException());

        await controlPanel.Refresh();
        controlPanel.Status.ShouldBe(Status.Failed);
        controlPanel.PreviouslyThrownException.ShouldNotBeNull();

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Failed);
        sf.Exception.ShouldNotBeNull();

        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task FailingExistingFunctionFromControlPanelSucceeds();
    protected async Task FailingExistingFunctionFromControlPanelSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var rFunc = functionsRegistry.RegisterFunc<string, string>(
            functionTypeId,
            string (string _) => throw new PostponeInvocationException(TimeSpan.FromMinutes(1))
        );
        
        await Should.ThrowAsync<Exception>(() => rFunc.Invoke(functionInstanceId.Value, ""));

        var controlPanel = await rFunc.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Postponed);
        controlPanel.PostponedUntil.ShouldNotBeNull();

        await controlPanel.Fail(new InvalidOperationException());

        await controlPanel.Refresh();
        controlPanel.Status.ShouldBe(Status.Failed);
        controlPanel.PreviouslyThrownException.ShouldNotBeNull();
        
        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Failed);
        sf.Exception.ShouldNotBeNull();
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task SucceedingExistingActionFromControlPanelSucceeds();
    protected async Task SucceedingExistingActionFromControlPanelSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
            void (string _) => throw new Exception("oh no")
        );
        
        await Should.ThrowAsync<Exception>(() => rAction.Invoke(functionInstanceId.Value, ""));

        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Failed);
        controlPanel.PreviouslyThrownException.ShouldNotBeNull();

        await controlPanel.Succeed();

        await controlPanel.Refresh();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        
        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Succeeded);
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task SucceedingExistingParamlessFromControlPanelSucceeds();
    protected async Task SucceedingExistingParamlessFromControlPanelSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var paramlessRegistration = functionsRegistry.RegisterParamless(
            functionTypeId,
            inner: Task () => throw new Exception("oh no")
        );
        
        await Should.ThrowAsync<Exception>(() => paramlessRegistration.Invoke(functionInstanceId.Value));

        var controlPanel = await paramlessRegistration.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Failed);
        controlPanel.PreviouslyThrownException.ShouldNotBeNull();

        await controlPanel.Succeed();

        await controlPanel.Refresh();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        
        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Succeeded);
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task SucceedingExistingFunctionFromControlPanelSucceeds();
    protected async Task SucceedingExistingFunctionFromControlPanelSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var rFunc = functionsRegistry.RegisterFunc<string, string>(
            functionTypeId,
            string (_) => throw new Exception("oh no")
        );
        
        await Should.ThrowAsync<Exception>(() => rFunc.Invoke(functionInstanceId.Value, ""));

        var controlPanel = await rFunc.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Failed);
        controlPanel.PreviouslyThrownException.ShouldNotBeNull();

        await controlPanel.Succeed("hello world");

        await controlPanel.Refresh();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        controlPanel.Result.ShouldBe("hello world");
        
        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Succeeded);
        var result = DefaultSerializer.Instance.DeserializeResult<string>(sf.Result!);
        result.ShouldBe("hello world");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ReInvokingExistingActionFromControlPanelSucceeds();
    protected async Task ReInvokingExistingActionFromControlPanelSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
            void(string param, Workflow workflow) =>
            {
                var state = workflow.States.CreateOrGet<TestState>("State");
                state.Value = param;
                state.Save().Wait();
            });

        await rAction.Invoke(functionInstanceId.Value, param: "first");

        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        controlPanel.States.Get<TestState>("State")!.Value.ShouldBe("first");
        controlPanel.PreviouslyThrownException.ShouldBeNull();

        controlPanel.Param = "second";
        await controlPanel.SaveChanges();
        await controlPanel.Refresh();
        await controlPanel.ReInvoke();
        await controlPanel.Refresh();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        controlPanel.States.Get<TestState>("State")!.Value.ShouldBe("second");
        
        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Succeeded);
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }

    private class TestState : WorkflowState
    {
        public string? Value { get; set; }
    }
    
    public abstract Task ReInvokingExistingFunctionFromControlPanelSucceeds();
    protected async Task ReinvokingExistingFunctionFromControlPanelSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = functionsRegistry.RegisterFunc(
            functionTypeId,
            string (string param) => param
        );

        await rAction.Invoke(functionInstanceId.Value, param: "first");

        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        controlPanel.Result.ShouldBe("first");
        controlPanel.PreviouslyThrownException.ShouldBeNull();

        controlPanel.Param = "second";
        var result = await controlPanel.ReInvoke();
        result.ShouldBe("second");
        
        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Succeeded);
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ScheduleReInvokingExistingActionFromControlPanelSucceeds();
    protected async Task ScheduleReInvokingExistingActionFromControlPanelSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
            void(string param, Workflow workflow) =>
            {
                var state = workflow.States.CreateOrGet<TestState>("State");
                state.Value = param;
                state.Save().Wait();
            });

        await rAction.Invoke(functionInstanceId.Value, param: "first");

        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        controlPanel.States.Get<TestState>("State")!.Value.ShouldBe("first");
        controlPanel.PreviouslyThrownException.ShouldBeNull();

        controlPanel.Param = "second";
        await controlPanel.SaveChanges();
        await controlPanel.Refresh();
        await controlPanel.ScheduleReInvoke();

        await BusyWait.Until(() => store.GetFunction(functionId).SelectAsync(sf => sf?.Status == Status.Succeeded));
        await controlPanel.Refresh();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        controlPanel.States.Get<TestState>("State")!.Value.ShouldBe("second");
        
        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Succeeded);
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ScheduleReInvokingExistingFunctionFromControlPanelSucceeds();
    protected async Task ScheduleReInvokingExistingFunctionFromControlPanelSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = functionsRegistry.RegisterFunc(
            functionTypeId,
            string (string param) => param
        );

        await rAction.Invoke(functionInstanceId.Value, param: "first");

        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        controlPanel.Result.ShouldBe("first");
        controlPanel.PreviouslyThrownException.ShouldBeNull();

        controlPanel.Param = "second";
        await controlPanel.ScheduleReInvoke();
        await BusyWait.Until(() => store.GetFunction(functionId).SelectAsync(sf => sf?.Status == Status.Succeeded));
        await controlPanel.Refresh();
        controlPanel.Result.ShouldBe("second");
        
        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Succeeded);
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ScheduleReInvokingExistingActionFromControlPanelFailsWhenEpochIsNotAsExpected();
    protected async Task ScheduleReInvokingExistingActionFromControlPanelFailsWhenEpochIsNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
            void (string _) => {}
        );

        await rAction.Invoke(functionInstanceId.Value, param: "first");

        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();

        {
            var tempControlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
            await tempControlPanel.SaveChanges(); //increment epoch
        }
        
        controlPanel.Param = "second";
        await Should.ThrowAsync<ConcurrentModificationException>(() => controlPanel.ScheduleReInvoke());

        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ScheduleReInvokingExistingFunctionFromControlPanelFailsWhenEpochIsNotAsExpected();
    protected async Task ScheduleReInvokingExistingFunctionFromControlPanelFailsWhenEpochIsNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var rFunc = functionsRegistry.RegisterFunc(
            functionTypeId,
            string (string param) => param
        );

        await rFunc.Invoke(functionInstanceId.Value, param: "first");

        var controlPanel = await rFunc.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();

        {
            var tempControlPanel = await rFunc.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
            await tempControlPanel.SaveChanges(); //increment epoch
        }
        
        controlPanel.Param = "second";
        await Should.ThrowAsync<ConcurrentModificationException>(() => controlPanel.ScheduleReInvoke());

        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task WaitingForExistingFunctionFromControlPanelToCompleteSucceeds();
    protected async Task WaitingForExistingFunctionFromControlPanelToCompleteSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var flag = new SyncedFlag();
        var rFunc = functionsRegistry.RegisterFunc(
            functionTypeId,
            async Task<string> (string param) =>
            {
                await flag.WaitForRaised();
                return param;
            });

        await rFunc.Schedule(functionInstanceId.Value, param: "param");

        var controlPanel = await rFunc.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Executing);

        var completionTask = controlPanel.WaitForCompletion();
        await Task.Delay(10);
        completionTask.IsCompleted.ShouldBeFalse();
        flag.Raise();

        await BusyWait.UntilAsync(() => completionTask.IsCompleted);

        var result = await completionTask;
        result.ShouldBe("param");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task WaitingForExistingActionFromControlPanelToCompleteSucceeds();
    protected async Task WaitingForExistingActionFromControlPanelToCompleteSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var flag = new SyncedFlag();
        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
            Task(string param) => flag.WaitForRaised()
        );

        await rAction.Schedule(functionInstanceId.Value, param: "param");

        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Executing);

        var completionTask = controlPanel.WaitForCompletion();
        await Task.Delay(10);
        completionTask.IsCompleted.ShouldBeFalse();
        flag.Raise();

        await BusyWait.UntilAsync(() => completionTask.IsCompleted);

        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task LeaseIsUpdatedForExecutingFunc();
    protected async Task LeaseIsUpdatedForExecutingFunc(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var before = DateTime.UtcNow;
        
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch, leaseLength: TimeSpan.FromMilliseconds(250)));
        var flag = new SyncedFlag();
        var rFunc = functionsRegistry.RegisterFunc(
            functionTypeId,
            async Task<string> (string param) =>
            {
                await flag.WaitForRaised();
                return param;
            });

        await rFunc.Schedule(functionInstanceId.Value, param: "param");

        var controlPanel = await rFunc.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Executing);
        var curr = controlPanel.LeaseExpiration;
        curr.ShouldBeGreaterThan(before);
        while (controlPanel.LeaseExpiration == curr)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(100));
            await controlPanel.Refresh();
        }

        flag.Raise();
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task LeaseIsUpdatedForExecutingAction();
    protected async Task LeaseIsUpdatedForExecutingAction(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var before = DateTime.UtcNow;

        using var functionsRegistry = new FunctionsRegistry(
            store, new Settings(
                unhandledExceptionCatcher.Catch,
                leaseLength: TimeSpan.FromMilliseconds(250),
                enableWatchdogs: false
            )
        );
        var flag = new SyncedFlag();
        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
            async Task(string param) =>
            {
                await flag.WaitForRaised();
            });

        await rAction.Schedule(functionInstanceId.Value, param: "param");

        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Executing);
        var curr = controlPanel.LeaseExpiration;
        curr.ShouldBeGreaterThan(before);
        while (controlPanel.LeaseExpiration == curr)
        {
            await controlPanel.Refresh();
            await Task.Delay(TimeSpan.FromMilliseconds(100));
        }

        flag.Raise();
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ReInvokeRFuncSucceedsAfterSuccessfullySavingParamAndState();
    protected async Task ReInvokeRFuncSucceedsAfterSuccessfullySavingParamAndState(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rAction = functionsRegistry.RegisterFunc(
            functionTypeId,
            string (string param) => param
        );

        await rAction.Invoke(functionInstanceId.Value, param: "param");

        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        await controlPanel.SaveChanges();
        await controlPanel.ReInvoke().ShouldBeAsync("param");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ReInvokeRActionSucceedsAfterSuccessfullySavingParamAndState();
    protected async Task ReInvokeRActionSucceedsAfterSuccessfullySavingParamAndState(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
            void(string _) => { }
        );

        await rAction.Invoke(functionInstanceId.Value, param: "param");

        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        await controlPanel.SaveChanges();
        await controlPanel.ReInvoke();
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ControlPanelsExistingMessagesContainsPreviouslyAddedMessages();
    protected async Task ControlPanelsExistingMessagesContainsPreviouslyAddedMessages(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
            async Task(string param, Workflow workflow) =>
            {
                await workflow.Messages.AppendMessage(param);
            }
        );

        await rAction.Invoke(functionInstanceId.Value, param: "param");

        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        var existingMessages = controlPanel.Messages;
        existingMessages.Count().ShouldBe(1);
        existingMessages[0].ShouldBe("param");
        await existingMessages.Replace(0, "hello");

        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ExistingMessagesCanBeReplacedUsingControlPanel();
    protected async Task ExistingMessagesCanBeReplacedUsingControlPanel(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var first = true;
        var invocationCount = new SyncedCounter();
        var syncedList = new SyncedList<string>();
        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
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
                    var existing = messages.Select(e => e.ToString()!).Existing(out _);
                    syncedList.AddRange(existing);
                }
            }
        );

        await rAction.Invoke(functionInstanceId.Value, param: "param");

        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        var existingMessages = controlPanel.Messages;
        existingMessages.Count().ShouldBe(2);
        await existingMessages.Clear();

        await existingMessages.Append("hello to you", "1");
        await existingMessages.Append("hello from me", "2");
        
        await controlPanel.SaveChanges();
        await controlPanel.Refresh();
        await controlPanel.ReInvoke();
        
        controlPanel.Messages.Count().ShouldBe(2);
        
        syncedList.ShouldNotBeNull();
        if (syncedList.Count != 2)
            throw new Exception(
                $"Excepted only 2 messages (invocation count: {invocationCount.Current}) - there was: " + string.Join(", ", syncedList.Select(e => "'" + e.ToJson() + "'"))
            );
        
        syncedList.Count.ShouldBe(2);
        syncedList[0].ShouldBe("hello to you");
        syncedList[1].ShouldBe("hello from me");

        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ExistingMessagesAreNotAffectedByControlPanelSaveChangesInvocation();
    protected async Task ExistingMessagesAreNotAffectedByControlPanelSaveChangesInvocation(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var first = true;
        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
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

        await rAction.Invoke(functionInstanceId.Value, param: "param");

        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Param = "test";
        await controlPanel.SaveChanges();
        await controlPanel.Refresh();

        var messages = controlPanel.Messages.MessagesWithIdempotencyKeys;
        messages.Count.ShouldBe(2);
        messages[0].Message.ShouldBe("hello world");
        messages[0].IdempotencyKey.ShouldBe("1");
        messages[1].Message.ShouldBe("hello universe");
        messages[1].IdempotencyKey.ShouldBe("2");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ConcurrentModificationOfExistingMessagesCausesExceptionOnSaveChanges();
    protected async Task ConcurrentModificationOfExistingMessagesCausesExceptionOnSaveChanges(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
            Task(string param, Workflow workflow) => Task.Delay(1)
        );

        await rAction.Invoke(functionInstanceId.Value, param: "param");
        await store.MessageStore.AppendMessage(
            functionId,
            new StoredMessage("hello world".ToJson(), typeof(string).SimpleQualifiedName())
        );

        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        var existingMessages = controlPanel.Messages;
        existingMessages.Count().ShouldBe(1);

        await store.MessageStore.AppendMessage(
            functionId,
            new StoredMessage("hello universe".ToJson(), typeof(string).SimpleQualifiedName())
        );
        
        await existingMessages.Clear();
        await existingMessages.Append("hej verden");
        await existingMessages.Append("hej univers");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ConcurrentModificationOfExistingMessagesDoesNotCauseExceptionOnSaveChangesWhenMessagesAreNotReplaced();
    protected async Task ConcurrentModificationOfExistingMessagesDoesNotCauseExceptionOnSaveChangesWhenMessagesAreNotReplaced(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
            Task(string param, Workflow workflow) => Task.Delay(1)
        );

        await rAction.Invoke(functionInstanceId.Value, param: "param");
        await store.MessageStore.AppendMessage(
            functionId,
            new StoredMessage("hello world".ToJson(), typeof(string).SimpleQualifiedName())
        );

        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();

        await store.MessageStore.AppendMessage(
            functionId,
            new StoredMessage("hello universe".ToJson(), typeof(string).SimpleQualifiedName())
        );

        controlPanel.Param = "PARAM";
        await controlPanel.SaveChanges();
        var epoch = controlPanel.Epoch;
        var param = controlPanel.Param;
        await controlPanel.Refresh();
        controlPanel.Epoch.ShouldBe(epoch);
        controlPanel.Param.ShouldBe(param);

        var messages = controlPanel.Messages;
        messages.Count().ShouldBe(2);
        messages[0].ShouldBe("hello world");
        messages[1].ShouldBe("hello universe");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ConcurrentModificationOfExistingMessagesCausesExceptionOnSave();
    protected async Task ConcurrentModificationOfExistingMessagesCausesExceptionOnSave(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
            Task(string param, Workflow workflow) => Task.Delay(1)
        );

        await rAction.Invoke(functionInstanceId.Value, param: "param");
        await store.MessageStore.AppendMessage(
            functionId,
            new StoredMessage("hello world".ToJson(), typeof(string).SimpleQualifiedName())
        );

        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        var existingMessages = controlPanel.Messages;
        existingMessages.Count().ShouldBe(1);

        await store.MessageStore.AppendMessage(
            functionId,
            new StoredMessage("hello universe".ToJson(), typeof(string).SimpleQualifiedName())
        );
        
        await existingMessages.Clear();
        await existingMessages.Append("hej verden");
        await existingMessages.Append("hej univers");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ConcurrentModificationOfExistingMessagesDoesNotCauseExceptionOnSucceedWhenMessagesAreNotReplaced();
    protected async Task ConcurrentModificationOfExistingMessagesDoesNotCauseExceptionOnSucceedWhenMessagesAreNotReplaced(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
            Task(string param, Workflow workflow) => Task.Delay(1)
        );

        await rAction.Invoke(functionInstanceId.Value, param: "param");
        await store.MessageStore.AppendMessage(
            functionId,
            new StoredMessage("hello world".ToJson(), typeof(string).SimpleQualifiedName())
        );

        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();

        await store.MessageStore.AppendMessage(
            functionId,
            new StoredMessage("hello universe".ToJson(), typeof(string).SimpleQualifiedName())
        );

        controlPanel.Param = "PARAM";
        await controlPanel.Succeed();
        var epoch = controlPanel.Epoch;
        var param = controlPanel.Param;
        await controlPanel.Refresh();
        controlPanel.Epoch.ShouldBe(epoch);
        controlPanel.Param.ShouldBe(param);

        var messages = controlPanel.Messages;
        messages.Count().ShouldBe(2);
        messages[0].ShouldBe("hello world");
        messages[1].ShouldBe("hello universe");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ExistingMessagesCanBeReplaced();
    protected async Task ExistingMessagesCanBeReplaced(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
            Task(string param, Workflow workflow) => Task.CompletedTask
        );

        await rAction.Invoke(functionInstanceId.Value, param: "param");
        await rAction.MessageWriters
            .For(functionInstanceId.Value)
            .AppendMessage("hello world", idempotencyKey: "first");
            
        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        var existingMessages = controlPanel.Messages;
        var (message, idempotencyKey) = existingMessages.MessagesWithIdempotencyKeys.Single();
        message.ShouldBe("hello world");
        idempotencyKey.ShouldBe("first");

        await existingMessages.Clear();
        await existingMessages.Append("hello universe", idempotencyKey: "second");

        await controlPanel.Refresh();

        existingMessages = controlPanel.Messages;
        (message, idempotencyKey) = existingMessages.MessagesWithIdempotencyKeys.Single();
        message.ShouldBe("hello universe");
        idempotencyKey.ShouldBe("second");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ExistingEffectCanBeReplacedWithValue();
    protected async Task ExistingEffectCanBeReplacedWithValue(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var rFunc = functionsRegistry.RegisterFunc(
            functionTypeId,
            Task<string> (string param, Workflow workflow) 
                => workflow.Effect.Capture("Test", () => "EffectResult")
        );

        var result = await rFunc.Invoke(functionInstanceId.Value, param: "param");
        result.ShouldBe("EffectResult");
        
        var controlPanel = await rFunc.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        var effects = controlPanel.Effects;
        await effects.SetSucceeded(effectId: "Test", result: "ReplacedResult");

        result = await controlPanel.ReInvoke();
        result.ShouldBe("ReplacedResult");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task EffectCanBeStarted();
    protected async Task EffectCanBeStarted(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var runEffect = false;
        
        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
            Task (string param, Workflow workflow) 
                => runEffect 
                    ? workflow.Effect.Capture("Test", () => {}, ResiliencyLevel.AtMostOnce)
                    : Task.CompletedTask
        );

        await rAction.Invoke(functionInstanceId.Value, param: "param");
        
        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        var effects = controlPanel.Effects;
        await effects.SetStarted(effectId: "Test");
        
        runEffect = true;
        await Should.ThrowAsync<Exception>(controlPanel.ReInvoke());
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ExistingEffectCanBeReplaced();
    protected async Task ExistingEffectCanBeReplaced(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rFunc = functionsRegistry.RegisterAction(
            functionTypeId,
            Task (string param, Workflow workflow) 
                => workflow.Effect.Capture("Test", () => throw new InvalidOperationException("oh no"))
        );

        await Should.ThrowAsync<Exception>(rFunc.Invoke(functionInstanceId.Value, param: "param"));
        
        var controlPanel = await rFunc.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        var activities = controlPanel.Effects;
        await activities.SetSucceeded(effectId: "Test");
        
        await controlPanel.ReInvoke();
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ExistingEffectCanBeRemoved();
    protected async Task ExistingEffectCanBeRemoved(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var syncedCounter = new SyncedCounter();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rFunc = functionsRegistry.RegisterFunc(
            functionTypeId,
            Task<string> (string param, Workflow workflow) =>
                workflow.Effect.Capture("Test", () =>
                {
                    syncedCounter++;
                    return "EffectResult";
                })
        );

        var result = await rFunc.Invoke(functionInstanceId.Value, param: "param");
        result.ShouldBe("EffectResult");
        syncedCounter.Current.ShouldBe(1);

        var controlPanel = await rFunc.ControlPanel(functionInstanceId.Value);
        controlPanel.ShouldNotBeNull();
        result = await controlPanel.ReInvoke();
        result.ShouldBe("EffectResult");
        syncedCounter.Current.ShouldBe(1);
        
        await controlPanel.Refresh();
        var activities = controlPanel.Effects;
        await activities.Remove("Test");

        await controlPanel.ReInvoke();

        result = await rFunc.Invoke(functionInstanceId.Value, param: "param");
        result.ShouldBe("EffectResult");
        syncedCounter.Current.ShouldBe(2);

        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task EffectsAreUpdatedAfterRefresh();
    protected async Task EffectsAreUpdatedAfterRefresh(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
            (string _, Workflow _) => {}
        );
        await rAction.Invoke(functionInstanceId.Value, param: "param");
        
        var firstControlPanel = await rAction.ControlPanel(functionInstanceId.Value);
        firstControlPanel.ShouldNotBeNull();
        
        var secondControlPanel = await rAction.ControlPanel(functionInstanceId.Value);
        secondControlPanel.ShouldNotBeNull();

        await firstControlPanel.Effects.SetSucceeded("Id", "SomeResult");
        
        secondControlPanel.Effects.HasValue("Id").ShouldBe(false);
        await secondControlPanel.Refresh();
        secondControlPanel.Effects.GetValue<string>("Id").ShouldBe("SomeResult");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ExistingEffectCanBeSetToFailed();
    protected async Task ExistingEffectCanBeSetToFailed(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var syncedCounter = new SyncedCounter();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rFunc = functionsRegistry.RegisterFunc(
            functionTypeId,
            Task<string> (string param, Workflow workflow) =>
                workflow.Effect.Capture("Test", () =>
                {
                    syncedCounter++;
                    return "EffectResult";
                })
        );

        var result = await rFunc.Invoke(functionInstanceId.Value, param: "param");
        result.ShouldBe("EffectResult");
        syncedCounter.Current.ShouldBe(1);

        var controlPanel = await rFunc.ControlPanel(functionInstanceId.Value);
        controlPanel.ShouldNotBeNull();
        var effects = controlPanel.Effects;
        await effects.SetFailed(effectId: "Test", new InvalidOperationException("oh no"));

        await Should.ThrowAsync<PreviousFunctionInvocationException>(() => 
            controlPanel.ReInvoke()
        );

        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    private class State : WorkflowState
    {
        public string Value { get; set; } = "";
    }
    
    public abstract Task ExistingStateCanBeReplacedRemovedAndAdded();
    protected async Task ExistingStateCanBeReplacedRemovedAndAdded(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var rFunc = functionsRegistry.RegisterFunc(
            functionTypeId,
            async Task<string> (string param, Workflow workflow) =>
            {
                var state = workflow.States.CreateOrGet<State>();
                state.Value = param;
                await state.Save();
                
                var namedState = workflow.States.CreateOrGet<State>("SomeId");
                namedState.Value = "NamedValue";
                await namedState.Save();
                
                return state.Value;
            });

        var result = await rFunc.Invoke(functionInstanceId.Value, param: "Some Param");
        result.ShouldBe("Some Param");
        
        var controlPanel = await rFunc.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        var otherControlPanel = await rFunc.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        
        var states = controlPanel.States;
        states.HasDefaultState().ShouldBeTrue();
        states.HasState("SomeId").ShouldBeTrue();
        states.Get<State>().Value.ShouldBe("Some Param");
        states.Get<State>(stateId: "SomeId").Value.ShouldBe("NamedValue");

        await states.Set(new State { Value = "New Value" });
        var s = states.Get<State>(stateId: "SomeId");
        s.Value = "New Value";
        await s.Save();
        states.Get<State>().Value.ShouldBe("New Value");

        await states.RemoveDefault();

        await states.Set("NewState", new State { Value = "NewState's Value" });
        
        await otherControlPanel.Refresh();

        states = otherControlPanel.States;
        states.HasDefaultState().ShouldBeFalse();
        states.HasState("SomeId").ShouldBeTrue();
        states.HasState("NewState").ShouldBeTrue();
        states.HasState("UnknownState").ShouldBeFalse();
        states.Get<State>(stateId: "SomeId").Value.ShouldBe("New Value");
        states.Get<State>(stateId: "NewState").Value.ShouldBe("NewState's Value");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task SaveChangesPersistsChangedResult();
    protected async Task SaveChangesPersistsChangedResult(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var rAction = functionsRegistry.RegisterFunc<string, string>(
            functionTypeId,
            inner: param => param
        );

        await rAction.Invoke(functionInstanceId.Value, param: "param");

        {
            var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
            controlPanel.Result.ShouldBe("param");
            await controlPanel.Succeed("changed");
        }
        
        {
            var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
            controlPanel.Result.ShouldBe("changed");
        }
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ExistingTimeoutCanBeUpdatedForAction();
    protected async Task ExistingTimeoutCanBeUpdatedForAction(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var actionRegistration = functionsRegistry.RegisterAction(
            functionTypeId,
            Task (string param, Workflow workflow) =>
                workflow.Messages.TimeoutProvider.RegisterTimeout(
                    "someTimeoutId", expiresAt: new DateTime(2100, 1,1, 1,1,1, DateTimeKind.Utc)
                )
        );

        await actionRegistration.Invoke(functionInstanceId.Value, param: "param");

        var controlPanel = await actionRegistration.ControlPanel(functionInstanceId.Value);
        controlPanel.ShouldNotBeNull();
        var timeouts = controlPanel.Timeouts;
        timeouts.All.Count.ShouldBe(1);
        timeouts["someTimeoutId"].ShouldBe(new DateTime(2100, 1,1, 1,1,1, DateTimeKind.Utc));

        await timeouts.Upsert("someOtherTimeoutId", new DateTime(2101, 1, 1, 1, 1, 1, DateTimeKind.Utc));
        timeouts.All.Count.ShouldBe(2);
        timeouts["someOtherTimeoutId"].ShouldBe(new DateTime(2101, 1,1, 1,1,1, DateTimeKind.Utc));
        
        await timeouts.Remove("someTimeoutId");
        timeouts.All.Count.ShouldBe(1);

        await controlPanel.Refresh();
        
        timeouts.All.Count.ShouldBe(1);
        timeouts["someOtherTimeoutId"].ShouldBe(new DateTime(2101, 1,1, 1,1,1, DateTimeKind.Utc));
    }
    
    public abstract Task ExistingTimeoutCanBeUpdatedForFunc();
    protected async Task ExistingTimeoutCanBeUpdatedForFunc(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var funcRegistration = functionsRegistry.RegisterFunc(
            functionTypeId,
            async Task<string> (string param, Workflow workflow) =>
            {
                await workflow.Messages.TimeoutProvider.RegisterTimeout(
                    "someTimeoutId", expiresAt: new DateTime(2100, 1, 1, 1, 1, 1, DateTimeKind.Utc)
                );

                return param;
            }
        );

        await funcRegistration.Invoke(functionInstanceId.Value, param: "param");

        var controlPanel = await funcRegistration.ControlPanel(functionInstanceId.Value);
        controlPanel.ShouldNotBeNull();
        var timeouts = controlPanel.Timeouts;
        timeouts.All.Count.ShouldBe(1);
        timeouts["someTimeoutId"].ShouldBe(new DateTime(2100, 1,1, 1,1,1, DateTimeKind.Utc));

        await timeouts.Upsert("someOtherTimeoutId", new DateTime(2101, 1, 1, 1, 1, 1, DateTimeKind.Utc));
        timeouts.All.Count.ShouldBe(2);
        timeouts["someOtherTimeoutId"].ShouldBe(new DateTime(2101, 1,1, 1,1,1, DateTimeKind.Utc));
        
        await timeouts.Remove("someTimeoutId");
        timeouts.All.Count.ShouldBe(1);

        await controlPanel.Refresh();
        
        timeouts.All.Count.ShouldBe(1);
        timeouts["someOtherTimeoutId"].ShouldBe(new DateTime(2101, 1,1, 1,1,1, DateTimeKind.Utc));
    }
    
    public abstract Task CorrelationsCanBeChanged();
    protected async Task CorrelationsCanBeChanged(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var registration = functionsRegistry.RegisterParamless(
            functionTypeId,
            async workflow =>
            {
                await workflow.Correlations.Register("SomeCorrelation");
                
            }
        );

        await registration.Invoke(functionInstanceId.Value);

        var controlPanel = await registration.ControlPanel(functionInstanceId.Value);
        controlPanel.ShouldNotBeNull();
       
        controlPanel.Correlations.Contains("SomeCorrelation").ShouldBeTrue();
        await controlPanel.Correlations.Remove("SomeCorrelation");
        await controlPanel.Correlations.Register("SomeNewCorrelation");

        controlPanel = await registration.ControlPanel(functionInstanceId.Value);
        controlPanel.ShouldNotBeNull();
        controlPanel.Correlations.Contains("SomeCorrelation").ShouldBeFalse();
        controlPanel.Correlations.Contains("SomeNewCorrelation").ShouldBeTrue();
    }
    
    public abstract Task DeleteRemovesFunctionFromAllStores();
    protected async Task DeleteRemovesFunctionFromAllStores(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var registration = functionsRegistry.RegisterParamless(
            functionTypeId,
            inner: () => Task.CompletedTask
        );

        await registration.Invoke(functionInstanceId.Value);

        var controlPanel = await registration.ControlPanel(functionInstanceId.Value);
        controlPanel.ShouldNotBeNull();

        await controlPanel.Correlations.Register("SomeCorrelation");
        await controlPanel.Effects.SetSucceeded("SomeEffect");
        await controlPanel.States.Set("SomeStateId", new TestState());
        await controlPanel.Messages.Append("Some Message");
        await controlPanel.Timeouts.Upsert("SomeTimeout", expiresAt: DateTime.UtcNow.Add(TimeSpan.FromDays(1)));
        
        await controlPanel.Delete();

        await store.GetFunction(functionId).ShouldBeNullAsync();
        
        await store.MessageStore.GetMessages(functionId, skip: 0)
            .SelectAsync(msgs => msgs.Count == 0)
            .ShouldBeTrueAsync();

        await store.TimeoutStore.GetTimeouts(functionId)
            .SelectAsync(ts => ts.Any())
            .ShouldBeFalseAsync();

        await store.StatesStore.GetStates(functionId)
            .SelectAsync(s => s.Any())
            .ShouldBeFalseAsync();

        await store.CorrelationStore.GetCorrelations(functionId)
            .SelectAsync(c => c.Any())
            .ShouldBeFalseAsync();

        await store.EffectsStore
            .GetEffectResults(functionId)
            .SelectAsync(e => e.Any())
            .ShouldBeFalseAsync();
    }
}