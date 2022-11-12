﻿using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
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
        const string functionInstanceId = "someFunctionId";
        var functionTypeId = nameof(ExistingActionCanBeDeletedFromControlPanel).ToFunctionTypeId();
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            (string _) => { }
        );
        
        await rAction.Invoke(functionInstanceId, "");

        var controlPanel = await rAction.ControlPanel.For(functionInstanceId).ShouldNotBeNullAsync();
        await controlPanel.Delete().ShouldBeTrueAsync();

        await Should.ThrowAsync<UnexpectedFunctionState>(controlPanel.Refresh());

        await store.GetFunction(functionId).ShouldBeNullAsync();
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ExistingFunctionCanBeDeletedFromControlPanel();
    protected async Task ExistingFunctionCanBeDeletedFromControlPanel(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        const string functionInstanceId = "someFunctionId";
        var functionTypeId = nameof(ExistingFunctionCanBeDeletedFromControlPanel).ToFunctionTypeId();
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rFunc = rFunctions.RegisterFunc(
            functionTypeId,
            string(string _) => "hello"
        );
        
        await rFunc.Invoke(functionInstanceId, "");

        var controlPanel = await rFunc.ControlPanel.For(functionInstanceId).ShouldNotBeNullAsync();
        await controlPanel.Delete().ShouldBeTrueAsync();

        await Should.ThrowAsync<UnexpectedFunctionState>(controlPanel.Refresh());

        await store.GetFunction(functionId).ShouldBeNullAsync();
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task DeletingExistingActionWithHigherEpochReturnsFalse();
    protected async Task DeletingExistingActionWithHigherEpochReturnsFalse(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        const string functionInstanceId = "someFunctionId";
        var functionTypeId = nameof(DeletingExistingActionWithHigherEpochReturnsFalse).ToFunctionTypeId();
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            (string _) => { }
        );
        
        await rAction.Invoke(functionInstanceId, "");

        var controlPanel = await rAction.ControlPanel.For(functionInstanceId).ShouldNotBeNullAsync();
        await store.IncrementEpoch(functionId).ShouldBeTrueAsync();
        await controlPanel.SaveParameterAndScrapbook(); //bump epoch

        await controlPanel.Delete().ShouldBeFalseAsync();
        await store.GetFunction(functionId).ShouldNotBeNullAsync();
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task DeletingExistingFuncWithHigherEpochReturnsFalse();
    protected async Task DeletingExistingFuncWithHigherEpochReturnsFalse(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        const string functionInstanceId = "someFunctionId";
        var functionTypeId = nameof(DeletingExistingFuncWithHigherEpochReturnsFalse).ToFunctionTypeId();
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rFunc = rFunctions.RegisterFunc(
            functionTypeId,
            string (string _) => "hello"
        );
        
        await rFunc.Invoke(functionInstanceId, "");

        var controlPanel = await rFunc.ControlPanel.For(functionInstanceId).ShouldNotBeNullAsync();

        await rFunc.ReInvoke(functionInstanceId, expectedEpoch: 0); //bump epoch

        await controlPanel.Delete().ShouldBeFalseAsync();
        await store.GetFunction(functionId).ShouldNotBeNullAsync();
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task PostponingExistingActionFromControlPanelSucceeds();
    protected async Task PostponingExistingActionFromControlPanelSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();

        var store = await storeTask;
        const string functionInstanceId = "someFunctionId";
        var functionTypeId = nameof(PostponingExistingActionFromControlPanelSucceeds).ToFunctionTypeId();
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            void (string _) => throw new Exception("oh no")
        );
        
        await Should.ThrowAsync<Exception>(() => rAction.Invoke(functionInstanceId, ""));

        var controlPanel = await rAction.ControlPanel.For(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Failed);
        controlPanel.PreviouslyThrownException.ShouldNotBeNull();
        
        await controlPanel.Postpone(new DateTime(1_000_000)).ShouldBeTrueAsync();

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
        const string functionInstanceId = "someFunctionId";
        var functionTypeId = nameof(PostponingExistingFunctionFromControlPanelSucceeds).ToFunctionTypeId();
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rFunc = rFunctions.RegisterFunc<string, string>(
            functionTypeId,
            string (_) => throw new Exception("oh no")
        );
        
        await Should.ThrowAsync<Exception>(() => rFunc.Invoke(functionInstanceId, ""));

        var controlPanel = await rFunc.ControlPanel.For(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Failed);
        controlPanel.PreviouslyThrownException.ShouldNotBeNull();
        
        await controlPanel.Postpone(new DateTime(1_000_000)).ShouldBeTrueAsync();

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
        const string functionInstanceId = "someFunctionId";
        var functionTypeId = nameof(FailingExistingActionFromControlPanelSucceeds).ToFunctionTypeId();
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            void (string _) => throw new PostponeInvocationException(TimeSpan.FromMinutes(1))
        );
        
        await Should.ThrowAsync<Exception>(() => rAction.Invoke(functionInstanceId, ""));

        var controlPanel = await rAction.ControlPanel.For(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Postponed);
        controlPanel.PostponedUntil.ShouldNotBeNull();
        
        await controlPanel.Fail(new InvalidOperationException()).ShouldBeTrueAsync();

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
        const string functionInstanceId = "someFunctionId";
        var functionTypeId = nameof(FailingExistingFunctionFromControlPanelSucceeds).ToFunctionTypeId();
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rFunc = rFunctions.RegisterFunc<string, string>(
            functionTypeId,
            string (string _) => throw new PostponeInvocationException(TimeSpan.FromMinutes(1))
        );
        
        await Should.ThrowAsync<Exception>(() => rFunc.Invoke(functionInstanceId, ""));

        var controlPanel = await rFunc.ControlPanel.For(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Postponed);
        controlPanel.PostponedUntil.ShouldNotBeNull();

        await controlPanel.Fail(new InvalidOperationException()).ShouldBeTrueAsync();

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
        const string functionInstanceId = "someFunctionId";
        var functionTypeId = nameof(SucceedingExistingActionFromControlPanelSucceeds).ToFunctionTypeId();
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            void (string _) => throw new Exception("oh no")
        );
        
        await Should.ThrowAsync<Exception>(() => rAction.Invoke(functionInstanceId, ""));

        var controlPanel = await rAction.ControlPanel.For(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Failed);
        controlPanel.PreviouslyThrownException.ShouldNotBeNull();

        await controlPanel.Succeed().ShouldBeTrueAsync();

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
        const string functionInstanceId = "someFunctionId";
        var functionTypeId = nameof(SucceedingExistingFunctionFromControlPanelSucceeds).ToFunctionTypeId();
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rFunc = rFunctions.RegisterFunc<string, string>(
            functionTypeId,
            string (_) => throw new Exception("oh no")
        );
        
        await Should.ThrowAsync<Exception>(() => rFunc.Invoke(functionInstanceId, ""));

        var controlPanel = await rFunc.ControlPanel.For(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Failed);
        controlPanel.PreviouslyThrownException.ShouldNotBeNull();

        await controlPanel.Succeed("hello world").ShouldBeTrueAsync();

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
        const string functionInstanceId = "someFunctionId";
        var functionTypeId = nameof(ReInvokingExistingActionFromControlPanelSucceeds).ToFunctionTypeId();
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            void(string param, RScrapbook scrapbook) =>
            {
                scrapbook.StateDictionary.Clear();
                scrapbook.StateDictionary["Value"] = param;
            }
        );

        await rAction.Invoke(functionInstanceId, param: "first");

        var controlPanel = await rAction.ControlPanel.For(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        controlPanel.Scrapbook.StateDictionary["Value"].ShouldBe("first");
        controlPanel.PreviouslyThrownException.ShouldBeNull();

        controlPanel.Param = "second";
        await controlPanel.SaveParameterAndScrapbook().ShouldBeTrueAsync();
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
        const string functionInstanceId = "someFunctionId";
        var functionTypeId = nameof(ReInvokingExistingFunctionFromControlPanelSucceeds).ToFunctionTypeId();
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = rFunctions.RegisterFunc(
            functionTypeId,
            string (string param) => param
        );

        await rAction.Invoke(functionInstanceId, param: "first");

        var controlPanel = await rAction.ControlPanel.For(functionInstanceId).ShouldNotBeNullAsync();
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
        const string functionInstanceId = "someFunctionId";
        var functionTypeId = nameof(ScheduleReInvokingExistingActionFromControlPanelSucceeds).ToFunctionTypeId();
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            void(string param, RScrapbook scrapbook) =>
            {
                scrapbook.StateDictionary.Clear();
                scrapbook.StateDictionary["Value"] = param;
            }
        );

        await rAction.Invoke(functionInstanceId, param: "first");

        var controlPanel = await rAction.ControlPanel.For(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        controlPanel.Scrapbook.StateDictionary["Value"].ShouldBe("first");
        controlPanel.PreviouslyThrownException.ShouldBeNull();

        controlPanel.Param = "second";
        await controlPanel.SaveParameterAndScrapbook().ShouldBeTrueAsync();
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
        const string functionInstanceId = "someFunctionId";
        var functionTypeId = nameof(ScheduleReInvokingExistingFunctionFromControlPanelSucceeds).ToFunctionTypeId();
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = rFunctions.RegisterFunc(
            functionTypeId,
            string (string param) => param
        );

        await rAction.Invoke(functionInstanceId, param: "first");

        var controlPanel = await rAction.ControlPanel.For(functionInstanceId).ShouldNotBeNullAsync();
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
        const string functionInstanceId = "someFunctionId";
        var functionTypeId = nameof(ScheduleReInvokingExistingFunctionFromControlPanelFailsWhenEpochIsNotAsExpected).ToFunctionTypeId();
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            void (string _) => {}
        );

        await rAction.Invoke(functionInstanceId, param: "first");

        var controlPanel = await rAction.ControlPanel.For(functionInstanceId).ShouldNotBeNullAsync();

        {
            var tempControlPanel = await rAction.ControlPanel.For(functionInstanceId).ShouldNotBeNullAsync();
            await tempControlPanel.SaveParameterAndScrapbook().ShouldBeTrueAsync(); //increment epoch
        }
        
        controlPanel.Param = "second";
        await Should.ThrowAsync<UnexpectedFunctionState>(() => controlPanel.ScheduleReInvoke());

        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ScheduleReInvokingExistingFunctionFromControlPanelFailsWhenEpochIsNotAsExpected();
    protected async Task ScheduleReInvokingExistingFunctionFromControlPanelFailsWhenEpochIsNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        const string functionInstanceId = "someFunctionId";
        var functionTypeId = nameof(ScheduleReInvokingExistingFunctionFromControlPanelFailsWhenEpochIsNotAsExpected).ToFunctionTypeId();
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rFunc = rFunctions.RegisterFunc(
            functionTypeId,
            string (string param) => param
        );

        await rFunc.Invoke(functionInstanceId, param: "first");

        var controlPanel = await rFunc.ControlPanel.For(functionInstanceId).ShouldNotBeNullAsync();

        {
            var tempControlPanel = await rFunc.ControlPanel.For(functionInstanceId).ShouldNotBeNullAsync();
            await tempControlPanel.SaveParameterAndScrapbook().ShouldBeTrueAsync(); //increment epoch
        }
        
        controlPanel.Param = "second";
        await Should.ThrowAsync<UnexpectedFunctionState>(() => controlPanel.ScheduleReInvoke());

        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task WaitingForExistingFunctionFromControlPanelToCompleteSucceeds();
    protected async Task WaitingForExistingFunctionFromControlPanelToCompleteSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        const string functionInstanceId = "someFunctionId";
        var functionTypeId = nameof(WaitingForExistingFunctionFromControlPanelToCompleteSucceeds).ToFunctionTypeId();
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var flag = new SyncedFlag();
        var rFunc = rFunctions.RegisterFunc(
            functionTypeId,
            async Task<string> (string param) =>
            {
                await flag.WaitForRaised();
                return param;
            });

        await rFunc.Schedule(functionInstanceId, param: "param");

        var controlPanel = await rFunc.ControlPanel.For(functionInstanceId).ShouldNotBeNullAsync();
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
        const string functionInstanceId = "someFunctionId";
        var functionTypeId = nameof(WaitingForExistingActionFromControlPanelToCompleteSucceeds).ToFunctionTypeId();
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var flag = new SyncedFlag();
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            Task(string param) => flag.WaitForRaised()
        );

        await rAction.Schedule(functionInstanceId, param: "param");

        var controlPanel = await rAction.ControlPanel.For(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Executing);

        var completionTask = controlPanel.WaitForCompletion();
        await Task.Delay(10);
        completionTask.IsCompleted.ShouldBeFalse();
        flag.Raise();

        await BusyWait.UntilAsync(() => completionTask.IsCompleted);

        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ReInvokeRFuncSucceedsAfterSuccessfullySavingParamAndScrapbook();
    protected async Task ReInvokeRFuncSucceedsAfterSuccessfullySavingParamAndScrapbook(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        const string functionInstanceId = "someFunctionId";
        var functionTypeId = nameof(ReInvokeRFuncSucceedsAfterSuccessfullySavingParamAndScrapbook).ToFunctionTypeId();
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));

        var rAction = rFunctions.RegisterFunc(
            functionTypeId,
            string (string param) => param
        );

        await rAction.Invoke(functionInstanceId, param: "param");

        var controlPanel = await rAction.ControlPanel.For(functionInstanceId).ShouldNotBeNullAsync();
        await controlPanel.SaveParameterAndScrapbook().ShouldBeTrueAsync();
        await controlPanel.ReInvoke().ShouldBeAsync("param");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ReInvokeRActionSucceedsAfterSuccessfullySavingParamAndScrapbook();
    protected async Task ReInvokeRActionSucceedsAfterSuccessfullySavingParamAndScrapbook(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        const string functionInstanceId = "someFunctionId";
        var functionTypeId = nameof(ReInvokeRFuncSucceedsAfterSuccessfullySavingParamAndScrapbook).ToFunctionTypeId();
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));

        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            void(string _) => { }
        );

        await rAction.Invoke(functionInstanceId, param: "param");

        var controlPanel = await rAction.ControlPanel.For(functionInstanceId).ShouldNotBeNullAsync();
        await controlPanel.SaveParameterAndScrapbook().ShouldBeTrueAsync();
        await controlPanel.ReInvoke();
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
}