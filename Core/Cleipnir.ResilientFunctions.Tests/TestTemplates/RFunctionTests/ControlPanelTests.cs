using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Reactive;
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
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            (string _) => { }
        );
        
        await rAction.Invoke(functionInstanceId.Value, "");

        var controlPanel = await rAction.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
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
        
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rFunc = rFunctions.RegisterFunc(
            functionTypeId,
            string(string _) => "hello"
        );
        
        await rFunc.Invoke(functionInstanceId.Value, "");

        var controlPanel = await rFunc.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
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
        
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            (string _) => { }
        );
        
        await rAction.Invoke(functionInstanceId.Value, "");

        var controlPanel = await rAction.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
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
        
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rFunc = rFunctions.RegisterFunc(
            functionTypeId,
            string (string _) => "hello"
        );
        
        await rFunc.Invoke(functionInstanceId.Value, "");

        var controlPanel = await rFunc.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();

        { //bump epoch
            var tempControlPanel = await rFunc.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
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
        
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            void (string _) => throw new Exception("oh no")
        );
        
        await Should.ThrowAsync<Exception>(() => rAction.Invoke(functionInstanceId.Value, ""));

        var controlPanel = await rAction.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
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
        
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rFunc = rFunctions.RegisterFunc<string, string>(
            functionTypeId,
            string (_) => throw new Exception("oh no")
        );
        
        await Should.ThrowAsync<Exception>(() => rFunc.Invoke(functionInstanceId.Value, ""));

        var controlPanel = await rFunc.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
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
        
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            void (string _) => throw new PostponeInvocationException(TimeSpan.FromMinutes(1))
        );
        
        await Should.ThrowAsync<Exception>(() => rAction.Invoke(functionInstanceId.Value, ""));

        var controlPanel = await rAction.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
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
        
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rFunc = rFunctions.RegisterFunc<string, string>(
            functionTypeId,
            string (string _) => throw new PostponeInvocationException(TimeSpan.FromMinutes(1))
        );
        
        await Should.ThrowAsync<Exception>(() => rFunc.Invoke(functionInstanceId.Value, ""));

        var controlPanel = await rFunc.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
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
        
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            void (string _) => throw new Exception("oh no")
        );
        
        await Should.ThrowAsync<Exception>(() => rAction.Invoke(functionInstanceId.Value, ""));

        var controlPanel = await rAction.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
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
        
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rFunc = rFunctions.RegisterFunc<string, string>(
            functionTypeId,
            string (_) => throw new Exception("oh no")
        );
        
        await Should.ThrowAsync<Exception>(() => rFunc.Invoke(functionInstanceId.Value, ""));

        var controlPanel = await rFunc.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
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
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            void(string param, RScrapbook scrapbook) =>
            {
                scrapbook.StateDictionary.Clear();
                scrapbook.StateDictionary["Value"] = param;
            }
        );

        await rAction.Invoke(functionInstanceId.Value, param: "first");

        var controlPanel = await rAction.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        controlPanel.Scrapbook.StateDictionary["Value"].ShouldBe("first");
        controlPanel.PreviouslyThrownException.ShouldBeNull();

        controlPanel.Param = "second";
        await controlPanel.SaveChanges();
        await controlPanel.Refresh();
        await controlPanel.ReInvoke();
        await controlPanel.Refresh();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        controlPanel.Scrapbook.StateDictionary["Value"].ShouldBe("second");
        
        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Succeeded);
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ReInvokingExistingFunctionFromControlPanelSucceeds();
    protected async Task ReinvokingExistingFunctionFromControlPanelSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = rFunctions.RegisterFunc(
            functionTypeId,
            string (string param) => param
        );

        await rAction.Invoke(functionInstanceId.Value, param: "first");

        var controlPanel = await rAction.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
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
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            void(string param, RScrapbook scrapbook) =>
            {
                scrapbook.StateDictionary.Clear();
                scrapbook.StateDictionary["Value"] = param;
            }
        );

        await rAction.Invoke(functionInstanceId.Value, param: "first");

        var controlPanel = await rAction.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        controlPanel.Scrapbook.StateDictionary["Value"].ShouldBe("first");
        controlPanel.PreviouslyThrownException.ShouldBeNull();

        controlPanel.Param = "second";
        await controlPanel.SaveChanges();
        await controlPanel.Refresh();
        await controlPanel.ScheduleReInvoke();

        await BusyWait.Until(() => store.GetFunction(functionId).SelectAsync(sf => sf?.Status == Status.Succeeded));
        await controlPanel.Refresh();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        controlPanel.Scrapbook.StateDictionary["Value"].ShouldBe("second");
        
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
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = rFunctions.RegisterFunc(
            functionTypeId,
            string (string param) => param
        );

        await rAction.Invoke(functionInstanceId.Value, param: "first");

        var controlPanel = await rAction.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
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
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            void (string _) => {}
        );

        await rAction.Invoke(functionInstanceId.Value, param: "first");

        var controlPanel = await rAction.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();

        {
            var tempControlPanel = await rAction.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
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
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rFunc = rFunctions.RegisterFunc(
            functionTypeId,
            string (string param) => param
        );

        await rFunc.Invoke(functionInstanceId.Value, param: "first");

        var controlPanel = await rFunc.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();

        {
            var tempControlPanel = await rFunc.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
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
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var flag = new SyncedFlag();
        var rFunc = rFunctions.RegisterFunc(
            functionTypeId,
            async Task<string> (string param) =>
            {
                await flag.WaitForRaised();
                return param;
            });

        await rFunc.Schedule(functionInstanceId.Value, param: "param");

        var controlPanel = await rFunc.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
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
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var flag = new SyncedFlag();
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            Task(string param) => flag.WaitForRaised()
        );

        await rAction.Schedule(functionInstanceId.Value, param: "param");

        var controlPanel = await rAction.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Executing);

        var completionTask = controlPanel.WaitForCompletion();
        await Task.Delay(10);
        completionTask.IsCompleted.ShouldBeFalse();
        flag.Raise();

        await BusyWait.UntilAsync(() => completionTask.IsCompleted);

        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task LastSignOfLifeIsUpdatedForExecutingFunc();
    protected async Task LastSignOfLifeIsUpdatedForExecutingFunc(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var before = DateTime.UtcNow;
        
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch, signOfLifeFrequency: TimeSpan.FromMilliseconds(250)));
        var flag = new SyncedFlag();
        var rFunc = rFunctions.RegisterFunc(
            functionTypeId,
            async Task<string> (string param) =>
            {
                await flag.WaitForRaised();
                return param;
            });

        await rFunc.Schedule(functionInstanceId.Value, param: "param");

        var controlPanel = await rFunc.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
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
    
    public abstract Task LastSignOfLifeIsUpdatedForExecutingAction();
    protected async Task LastSignOfLifeIsUpdatedForExecutingAction(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var before = DateTime.UtcNow;
        
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch, signOfLifeFrequency: TimeSpan.FromMilliseconds(250)));
        var flag = new SyncedFlag();
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            async Task(string param) =>
            {
                await flag.WaitForRaised();
            });

        await rAction.Schedule(functionInstanceId.Value, param: "param");

        var controlPanel = await rAction.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
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
    
    public abstract Task ReInvokeRFuncSucceedsAfterSuccessfullySavingParamAndScrapbook();
    protected async Task ReInvokeRFuncSucceedsAfterSuccessfullySavingParamAndScrapbook(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));

        var rAction = rFunctions.RegisterFunc(
            functionTypeId,
            string (string param) => param
        );

        await rAction.Invoke(functionInstanceId.Value, param: "param");

        var controlPanel = await rAction.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
        await controlPanel.SaveChanges();
        await controlPanel.ReInvoke().ShouldBeAsync("param");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ReInvokeRActionSucceedsAfterSuccessfullySavingParamAndScrapbook();
    protected async Task ReInvokeRActionSucceedsAfterSuccessfullySavingParamAndScrapbook(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));

        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            void(string _) => { }
        );

        await rAction.Invoke(functionInstanceId.Value, param: "param");

        var controlPanel = await rAction.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
        await controlPanel.SaveChanges();
        await controlPanel.ReInvoke();
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ControlPanelsExistingEventsContainsPreviouslyAddedEvents();
    protected async Task ControlPanelsExistingEventsContainsPreviouslyAddedEvents(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            async Task(string param, RScrapbook _, Context context) =>
            {
                using var eventSource = await context.EventSource;
                await eventSource.AppendEvent(param);
            }
        );

        await rAction.Invoke(functionInstanceId.Value, param: "param");

        var controlPanel = await rAction.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
        var existingEvents = await controlPanel.Events;
        existingEvents.Count().ShouldBe(1);
        existingEvents[0].ShouldBe("param");
        existingEvents[0] = "hello";

        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ExistingEventsCanBeReplacedUsingControlPanel();
    protected async Task ExistingEventsCanBeReplacedUsingControlPanel(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));

        var first = true;
        var invocationCount = new SyncedCounter();
        var syncedList = new SyncedList<string>();
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            async Task(string param, RScrapbook _, Context context) =>
            {
                using var eventSource = await context.EventSource;
                if (first)
                {
                    invocationCount.Increment();
                    first = false;
                    await eventSource.AppendEvent("hello world", idempotencyKey: "1");
                    await eventSource.AppendEvent("hello universe", idempotencyKey: "2");
                }
                else
                {
                    var existingEvents = eventSource.Select(e => e.ToString()!).PullExisting();
                    syncedList.AddRange(existingEvents);
                }
            }
        );

        await rAction.Invoke(functionInstanceId.Value, param: "param");

        var controlPanel = await rAction.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        var existingEvents = await controlPanel.Events;
        existingEvents.Count().ShouldBe(2);
        existingEvents.Clear();
        existingEvents.EventsWithIdempotencyKeys.Add(new EventAndIdempotencyKey("hello to you", "1"));
        existingEvents.EventsWithIdempotencyKeys.Add(new EventAndIdempotencyKey("hello from me", "2"));
        await existingEvents.SaveChanges(verifyNoChangesBeforeSave: true);
        
        await controlPanel.SaveChanges();
        await controlPanel.Refresh();
        await controlPanel.ReInvoke();
        
        (await controlPanel.Events).Count().ShouldBe(2);
        
        syncedList.ShouldNotBeNull();
        if (syncedList.Count != 2)
            throw new Exception(
                $"Excepted only 2 events (invocation count: {invocationCount.Current}) - there was: " + string.Join(", ", syncedList.Select(e => "'" + e.ToJson() + "'"))
            );
        
        syncedList.Count.ShouldBe(2);
        syncedList[0].ShouldBe("hello to you");
        syncedList[1].ShouldBe("hello from me");

        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ExistingEventsAreNotAffectedByControlPanelSaveChangesInvocation();
    protected async Task ExistingEventsAreNotAffectedByControlPanelSaveChangesInvocation(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));

        var first = true;
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            async Task(string param, RScrapbook _, Context context) =>
            {
                using var eventSource = await context.EventSource;
                if (first)
                {
                    first = false;
                    await eventSource.AppendEvent("hello world", idempotencyKey: "1");
                    await eventSource.AppendEvent("hello universe", idempotencyKey: "2");
                }
            }
        );

        await rAction.Invoke(functionInstanceId.Value, param: "param");

        var controlPanel = await rAction.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Param = "test";
        await controlPanel.SaveChanges();
        await controlPanel.Refresh();

        var events = (await controlPanel.Events).EventsWithIdempotencyKeys;
        events.Count.ShouldBe(2);
        events[0].Event.ShouldBe("hello world");
        events[0].IdempotencyKey.ShouldBe("1");
        events[1].Event.ShouldBe("hello universe");
        events[1].IdempotencyKey.ShouldBe("2");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ConcurrentModificationOfExistingEventsCausesExceptionOnSaveChanges();
    protected async Task ConcurrentModificationOfExistingEventsCausesExceptionOnSaveChanges(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            Task(string param, RScrapbook _, Context context) => Task.Delay(1)
        );

        await rAction.Invoke(functionInstanceId.Value, param: "param");
        await store.EventStore.AppendEvent(functionId, "hello world".ToJson(), typeof(string).SimpleQualifiedName());

        var controlPanel = await rAction.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
        var existingEvents = await controlPanel.Events;
        existingEvents.Count().ShouldBe(1);

        await store.EventStore.AppendEvent(functionId, "hello universe".ToJson(), typeof(string).SimpleQualifiedName());
        
        existingEvents.Clear();
        existingEvents.Add("hej verden");
        existingEvents.Add("hej univers");
        
        await Should.ThrowAsync<ConcurrentModificationException>(() => existingEvents.SaveChanges(verifyNoChangesBeforeSave: true));
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ConcurrentModificationOfExistingEventsDoesNotCauseExceptionOnSaveChangesWhenEventsAreNotReplaced();
    protected async Task ConcurrentModificationOfExistingEventsDoesNotCauseExceptionOnSaveChangesWhenEventsAreNotReplaced(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            Task(string param, RScrapbook _, Context context) => Task.Delay(1)
        );

        await rAction.Invoke(functionInstanceId.Value, param: "param");
        await store.EventStore.AppendEvent(functionId, "hello world".ToJson(), typeof(string).SimpleQualifiedName());

        var controlPanel = await rAction.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();

        await store.EventStore.AppendEvent(functionId, "hello universe".ToJson(), typeof(string).SimpleQualifiedName());

        controlPanel.Param = "PARAM";
        await controlPanel.SaveChanges();
        var epoch = controlPanel.Epoch;
        var param = controlPanel.Param;
        await controlPanel.Refresh();
        controlPanel.Epoch.ShouldBe(epoch);
        controlPanel.Param.ShouldBe(param);

        var events = await controlPanel.Events;
        events.Count().ShouldBe(2);
        events[0].ShouldBe("hello world");
        events[1].ShouldBe("hello universe");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ConcurrentModificationOfExistingEventsCausesExceptionOnSave();
    protected async Task ConcurrentModificationOfExistingEventsCausesExceptionOnSave(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            Task(string param, RScrapbook _, Context context) => Task.Delay(1)
        );

        await rAction.Invoke(functionInstanceId.Value, param: "param");
        await store.EventStore.AppendEvent(functionId, "hello world".ToJson(), typeof(string).SimpleQualifiedName());

        var controlPanel = await rAction.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
        var existingEvents = await controlPanel.Events;
        existingEvents.Count().ShouldBe(1);

        await store.EventStore.AppendEvent(functionId, "hello universe".ToJson(), typeof(string).SimpleQualifiedName());
        
        existingEvents.Clear();
        existingEvents.Add("hej verden");
        existingEvents.Add("hej univers");

        await Should.ThrowAsync<ConcurrentModificationException>(() => existingEvents.SaveChanges(verifyNoChangesBeforeSave: true));
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ConcurrentModificationOfExistingEventsDoesNotCauseExceptionOnSucceedWhenEventsAreNotReplaced();
    protected async Task ConcurrentModificationOfExistingEventsDoesNotCauseExceptionOnSucceedWhenEventsAreNotReplaced(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            Task(string param, RScrapbook _, Context context) => Task.Delay(1)
        );

        await rAction.Invoke(functionInstanceId.Value, param: "param");
        await store.EventStore.AppendEvent(functionId, "hello world".ToJson(), typeof(string).SimpleQualifiedName());

        var controlPanel = await rAction.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();

        await store.EventStore.AppendEvent(functionId, "hello universe".ToJson(), typeof(string).SimpleQualifiedName());

        controlPanel.Param = "PARAM";
        await controlPanel.Succeed();
        var epoch = controlPanel.Epoch;
        var param = controlPanel.Param;
        await controlPanel.Refresh();
        controlPanel.Epoch.ShouldBe(epoch);
        controlPanel.Param.ShouldBe(param);

        var events = await controlPanel.Events;
        events.Count().ShouldBe(2);
        events[0].ShouldBe("hello world");
        events[1].ShouldBe("hello universe");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ExistingEventsCanBeReplaced();
    protected async Task ExistingEventsCanBeReplaced(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            Task(string param, RScrapbook _, Context context) => Task.CompletedTask
        );

        await rAction.Invoke(
            functionInstanceId.Value,
            param: "param",
            events: new[] { new EventAndIdempotencyKey("hello world", IdempotencyKey: "first") }
        );
        
        var controlPanel = await rAction.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
        var existingEvents = await controlPanel.Events;
        var (@event, idempotencyKey) = existingEvents.EventsWithIdempotencyKeys.Single();
        @event.ShouldBe("hello world");
        idempotencyKey.ShouldBe("first");

        existingEvents.Clear();
        existingEvents.EventsWithIdempotencyKeys.Add(new EventAndIdempotencyKey("hello universe", IdempotencyKey: "second"));

        await existingEvents.SaveChanges();

        await controlPanel.Refresh();

        existingEvents = await controlPanel.Events;
        (@event, idempotencyKey) = existingEvents.EventsWithIdempotencyKeys.Single();
        @event.ShouldBe("hello universe");
        idempotencyKey.ShouldBe("second");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
}