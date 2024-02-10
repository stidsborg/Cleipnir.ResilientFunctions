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
            (string _) => { }
        );
        
        await rAction.Invoke(functionInstanceId.Value, "");

        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        await controlPanel.Delete();

        await Should.ThrowAsync<UnexpectedFunctionState>(controlPanel.Refresh());

        await store.GetFunction(functionId).ShouldBeNullAsync();
        
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
            string(string _) => "hello"
        );
        
        await rFunc.Invoke(functionInstanceId.Value, "");

        var controlPanel = await rFunc.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        await controlPanel.Delete();

        await Should.ThrowAsync<UnexpectedFunctionState>(controlPanel.Refresh());

        await store.GetFunction(functionId).ShouldBeNullAsync();
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task DeletingExistingActionWithHigherEpochReturnsFalse();
    protected async Task DeletingExistingActionWithHigherEpochReturnsFalse(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
            (string _) => { }
        );
        
        await rAction.Invoke(functionInstanceId.Value, "");

        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        await store.IncrementEpoch(functionId).ShouldBeTrueAsync();
        await Should.ThrowAsync<ConcurrentModificationException>(controlPanel.SaveChanges());

        await Should.ThrowAsync<ConcurrentModificationException>(() => controlPanel.Delete());
        await store.GetFunction(functionId).ShouldNotBeNullAsync();
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task DeletingExistingFuncWithHigherEpochReturnsFalse();
    protected async Task DeletingExistingFuncWithHigherEpochReturnsFalse(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var rFunc = functionsRegistry.RegisterFunc(
            functionTypeId,
            string (string _) => "hello"
        );
        
        await rFunc.Invoke(functionInstanceId.Value, "");

        var controlPanel = await rFunc.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();

        { //bump epoch
            var tempControlPanel = await rFunc.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
            await tempControlPanel.SaveChanges();
        }

        await Should.ThrowAsync<ConcurrentModificationException>(() => controlPanel.Delete());
        
        await store.GetFunction(functionId).ShouldNotBeNullAsync();
        
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
        var result = DefaultSerializer.Instance.DeserializeResult<string>(sf.Result.ResultJson!, sf.Result.ResultType!);
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
            void(string param, TestState state) => state.Value = param
        );

        await rAction.Invoke(functionInstanceId.Value, param: "first");

        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        controlPanel.State.Value.ShouldBe("first");
        controlPanel.PreviouslyThrownException.ShouldBeNull();

        controlPanel.Param = "second";
        await controlPanel.SaveChanges();
        await controlPanel.Refresh();
        await controlPanel.ReInvoke();
        await controlPanel.Refresh();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        controlPanel.State.Value.ShouldBe("second");
        
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
            void(string param, TestState state) => state.Value = param
        );

        await rAction.Invoke(functionInstanceId.Value, param: "first");

        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        controlPanel.State.Value.ShouldBe("first");
        controlPanel.PreviouslyThrownException.ShouldBeNull();

        controlPanel.Param = "second";
        await controlPanel.SaveChanges();
        await controlPanel.Refresh();
        await controlPanel.ScheduleReInvoke();

        await BusyWait.Until(() => store.GetFunction(functionId).SelectAsync(sf => sf?.Status == Status.Succeeded));
        await controlPanel.Refresh();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        controlPanel.State.Value.ShouldBe("second");
        
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
        
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch, leaseLength: TimeSpan.FromMilliseconds(250)));
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
            async Task(string param, WorkflowState _, Workflow workflow) =>
            {
                using var messages = workflow.Messages;
                await messages.AppendMessage(param);
            }
        );

        await rAction.Invoke(functionInstanceId.Value, param: "param");

        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        var existingMessages = controlPanel.Messages;
        existingMessages.Count().ShouldBe(1);
        existingMessages[0].ShouldBe("param");
        existingMessages[0] = "hello";

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
            async Task(string param, WorkflowState _, Workflow workflow) =>
            {
                using var messages = workflow.Messages;
                if (first)
                {
                    invocationCount.Increment();
                    first = false;
                    await messages.AppendMessage("hello world", idempotencyKey: "1");
                    await messages.AppendMessage("hello universe", idempotencyKey: "2");
                }
                else
                {
                    var existing = messages.Select(e => e.ToString()!).Existing();
                    syncedList.AddRange(existing);
                }
            }
        );

        await rAction.Invoke(functionInstanceId.Value, param: "param");

        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        var existingMessages = controlPanel.Messages;
        existingMessages.Count().ShouldBe(2);
        existingMessages.Clear();
        existingMessages.MessagesWithIdempotencyKeys.Add(new MessageAndIdempotencyKey("hello to you", "1"));
        existingMessages.MessagesWithIdempotencyKeys.Add(new MessageAndIdempotencyKey("hello from me", "2"));
        await existingMessages.SaveChanges(verifyNoChangesBeforeSave: true);
        
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
            async Task(string param, WorkflowState _, Workflow workflow) =>
            {
                using var messages = workflow.Messages;
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
            Task(string param, WorkflowState _, Workflow workflow) => Task.Delay(1)
        );

        await rAction.Invoke(functionInstanceId.Value, param: "param");
        await store.MessageStore.AppendMessage(functionId, "hello world".ToJson(), typeof(string).SimpleQualifiedName());

        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        var existingMessages = controlPanel.Messages;
        existingMessages.Count().ShouldBe(1);

        await store.MessageStore.AppendMessage(functionId, "hello universe".ToJson(), typeof(string).SimpleQualifiedName());
        
        existingMessages.Clear();
        existingMessages.Add("hej verden");
        existingMessages.Add("hej univers");
        
        await Should.ThrowAsync<ConcurrentModificationException>(() => existingMessages.SaveChanges(verifyNoChangesBeforeSave: true));
        
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
            Task(string param, WorkflowState _, Workflow workflow) => Task.Delay(1)
        );

        await rAction.Invoke(functionInstanceId.Value, param: "param");
        await store.MessageStore.AppendMessage(functionId, "hello world".ToJson(), typeof(string).SimpleQualifiedName());

        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();

        await store.MessageStore.AppendMessage(functionId, "hello universe".ToJson(), typeof(string).SimpleQualifiedName());

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
            Task(string param, WorkflowState _, Workflow workflow) => Task.Delay(1)
        );

        await rAction.Invoke(functionInstanceId.Value, param: "param");
        await store.MessageStore.AppendMessage(functionId, "hello world".ToJson(), typeof(string).SimpleQualifiedName());

        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        var existingMessages = controlPanel.Messages;
        existingMessages.Count().ShouldBe(1);

        await store.MessageStore.AppendMessage(functionId, "hello universe".ToJson(), typeof(string).SimpleQualifiedName());
        
        existingMessages.Clear();
        existingMessages.Add("hej verden");
        existingMessages.Add("hej univers");

        await Should.ThrowAsync<ConcurrentModificationException>(() => existingMessages.SaveChanges(verifyNoChangesBeforeSave: true));
        
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
            Task(string param, WorkflowState _, Workflow workflow) => Task.Delay(1)
        );

        await rAction.Invoke(functionInstanceId.Value, param: "param");
        await store.MessageStore.AppendMessage(functionId, "hello world".ToJson(), typeof(string).SimpleQualifiedName());

        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();

        await store.MessageStore.AppendMessage(functionId, "hello universe".ToJson(), typeof(string).SimpleQualifiedName());

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
            Task(string param, WorkflowState _, Workflow workflow) => Task.CompletedTask
        );

        await rAction.Invoke(functionInstanceId.Value, param: "param");
        await rAction.MessageWriters
            .For(functionInstanceId.Value)
            .AppendMessage(new MessageAndIdempotencyKey("hello world", IdempotencyKey: "first"));
            
        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        var existingMessages = controlPanel.Messages;
        var (message, idempotencyKey) = existingMessages.MessagesWithIdempotencyKeys.Single();
        message.ShouldBe("hello world");
        idempotencyKey.ShouldBe("first");

        existingMessages.Clear();
        existingMessages.MessagesWithIdempotencyKeys.Add(new MessageAndIdempotencyKey("hello universe", IdempotencyKey: "second"));

        await existingMessages.SaveChanges();

        await controlPanel.Refresh();

        existingMessages = controlPanel.Messages;
        (message, idempotencyKey) = existingMessages.MessagesWithIdempotencyKeys.Single();
        message.ShouldBe("hello universe");
        idempotencyKey.ShouldBe("second");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ExistingActivityCanBeReplacedWithValue();
    protected async Task ExistingActivityCanBeReplacedWithValue(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var rFunc = functionsRegistry.RegisterFunc(
            functionTypeId,
            Task<string> (string param, WorkflowState _, Workflow workflow) 
                => workflow.Activities.Do("Test", () => "ActivityResult")
        );

        var result = await rFunc.Invoke(functionInstanceId.Value, param: "param");
        result.ShouldBe("ActivityResult");
        
        var controlPanel = await rFunc.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        var activities = controlPanel.Activities;
        await activities.SetSucceeded(activityId: "Test", result: "ReplacedResult");

        result = await controlPanel.ReInvoke();
        result.ShouldBe("ReplacedResult");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ActivityCanBeStarted();
    protected async Task ActivityCanBeStarted(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var runActivity = false;
        
        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
            Task (string param, WorkflowState _, Workflow workflow) 
                => runActivity 
                    ? workflow.Activities.Do("Test", () => {}, ResiliencyLevel.AtMostOnce)
                    : Task.CompletedTask
        );

        await rAction.Invoke(functionInstanceId.Value, param: "param");
        
        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        var activities = controlPanel.Activities;
        await activities.SetStarted(activityId: "Test");
        
        runActivity = true;
        await Should.ThrowAsync<Exception>(controlPanel.ReInvoke());
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ExistingActivityCanBeReplaced();
    protected async Task ExistingActivityCanBeReplaced(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rFunc = functionsRegistry.RegisterAction(
            functionTypeId,
            Task (string param, WorkflowState _, Workflow workflow) 
                => workflow.Activities.Do("Test", () => throw new InvalidOperationException("oh no"))
        );

        await Should.ThrowAsync<Exception>(rFunc.Invoke(functionInstanceId.Value, param: "param"));
        
        var controlPanel = await rFunc.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        var activities = controlPanel.Activities;
        await activities.SetSucceeded(activityId: "Test");
        
        await controlPanel.ReInvoke();
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ExistingActivityCanBeRemoved();
    protected async Task ExistingActivityCanBeRemoved(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var syncedCounter = new SyncedCounter();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rFunc = functionsRegistry.RegisterFunc(
            functionTypeId,
            Task<string> (string param, WorkflowState _, Workflow workflow) =>
                workflow.Activities.Do("Test", () =>
                {
                    syncedCounter++;
                    return "ActivityResult";
                })
        );

        var result = await rFunc.Invoke(functionInstanceId.Value, param: "param");
        result.ShouldBe("ActivityResult");
        syncedCounter.Current.ShouldBe(1);

        var controlPanel = await rFunc.ControlPanel(functionInstanceId.Value);
        controlPanel.ShouldNotBeNull();
        result = await controlPanel.ReInvoke();
        result.ShouldBe("ActivityResult");
        syncedCounter.Current.ShouldBe(1);
        
        await controlPanel.Refresh();
        var activities = controlPanel.Activities;
        await activities.Remove("Test");

        await controlPanel.ReInvoke();

        result = await rFunc.Invoke(functionInstanceId.Value, param: "param");
        result.ShouldBe("ActivityResult");
        syncedCounter.Current.ShouldBe(2);

        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ActivitiesAreUpdatedAfterRefresh();
    protected async Task ActivitiesAreUpdatedAfterRefresh(Task<IFunctionStore> storeTask)
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

        await firstControlPanel.Activities.SetSucceeded("Id", "SomeResult");
        
        secondControlPanel.Activities.HasValue("Id").ShouldBe(false);
        await secondControlPanel.Refresh();
        secondControlPanel.Activities.GetValue<string>("Id").ShouldBe("SomeResult");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ExistingActivityCanBeSetToFailed();
    protected async Task ExistingActivityCanBeSetToFailed(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var syncedCounter = new SyncedCounter();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rFunc = functionsRegistry.RegisterFunc(
            functionTypeId,
            Task<string> (string param, WorkflowState _, Workflow workflow) =>
                workflow.Activities.Do("Test", () =>
                {
                    syncedCounter++;
                    return "ActivityResult";
                })
        );

        var result = await rFunc.Invoke(functionInstanceId.Value, param: "param");
        result.ShouldBe("ActivityResult");
        syncedCounter.Current.ShouldBe(1);

        var controlPanel = await rFunc.ControlPanel(functionInstanceId.Value);
        controlPanel.ShouldNotBeNull();
        var activities = controlPanel.Activities;
        await activities.SetFailed(activityId: "Test", new InvalidOperationException("oh no"));

        await Should.ThrowAsync<PreviousFunctionInvocationException>(() => 
            controlPanel.ReInvoke()
        );

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
            controlPanel.Result = "changed";
            await controlPanel.SaveChanges();
        }
        
        {
            var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
            controlPanel.Result.ShouldBe("changed");
        }
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
}