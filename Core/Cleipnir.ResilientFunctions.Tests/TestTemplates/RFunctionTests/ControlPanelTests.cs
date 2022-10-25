using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
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

        await rAction.ReInvoke(functionInstanceId, new[] { Status.Succeeded }); //bump epoch

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
        var functionTypeId = nameof(ExistingActionCanBeDeletedFromControlPanel).ToFunctionTypeId();
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            void (string _) => throw new Exception("oh no")
        );
        
        await Should.ThrowAsync<Exception>(() => rAction.Invoke(functionInstanceId, ""));

        var controlPanel = await rAction.ControlPanel.For(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Failed);
        controlPanel.FailedWithException.ShouldNotBeNull();
        
        await controlPanel.Postpone(new DateTime(1_000_000)).ShouldBeTrueAsync();

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
        var functionTypeId = nameof(ExistingActionCanBeDeletedFromControlPanel).ToFunctionTypeId();
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rFunc = rFunctions.RegisterFunc<string, string>(
            functionTypeId,
            string (_) => throw new Exception("oh no")
        );
        
        await Should.ThrowAsync<Exception>(() => rFunc.Invoke(functionInstanceId, ""));

        var controlPanel = await rFunc.ControlPanel.For(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Failed);
        controlPanel.FailedWithException.ShouldNotBeNull();
        
        await controlPanel.Postpone(new DateTime(1_000_000)).ShouldBeTrueAsync();

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
        var functionTypeId = nameof(ExistingActionCanBeDeletedFromControlPanel).ToFunctionTypeId();
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

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Failed);
        sf.ErrorJson.ShouldNotBeNull();

        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task FailingExistingFunctionFromControlPanelSucceeds();
    protected async Task FailingExistingFunctionFromControlPanelSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        const string functionInstanceId = "someFunctionId";
        var functionTypeId = nameof(ExistingActionCanBeDeletedFromControlPanel).ToFunctionTypeId();
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

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Failed);
        sf.ErrorJson.ShouldNotBeNull();
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task SucceedingExistingActionFromControlPanelSucceeds();
    protected async Task SucceedingExistingActionFromControlPanelSucceeds(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        const string functionInstanceId = "someFunctionId";
        var functionTypeId = nameof(ExistingActionCanBeDeletedFromControlPanel).ToFunctionTypeId();
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            void (string _) => throw new Exception("oh no")
        );
        
        await Should.ThrowAsync<Exception>(() => rAction.Invoke(functionInstanceId, ""));

        var controlPanel = await rAction.ControlPanel.For(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Failed);
        controlPanel.FailedWithException.ShouldNotBeNull();

        await controlPanel.Succeed().ShouldBeTrueAsync();

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
        var functionTypeId = nameof(ExistingActionCanBeDeletedFromControlPanel).ToFunctionTypeId();
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        var rFunc = rFunctions.RegisterFunc<string, string>(
            functionTypeId,
            string (_) => throw new Exception("oh no")
        );
        
        await Should.ThrowAsync<Exception>(() => rFunc.Invoke(functionInstanceId, ""));

        var controlPanel = await rFunc.ControlPanel.For(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Failed);
        controlPanel.FailedWithException.ShouldNotBeNull();

        await controlPanel.Succeed("hello world").ShouldBeTrueAsync();

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Succeeded);
        var result = DefaultSerializer.Instance.DeserializeResult<string>(sf.Result.ResultJson!, sf.Result.ResultType!);
        result.ShouldBe("hello world");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
}