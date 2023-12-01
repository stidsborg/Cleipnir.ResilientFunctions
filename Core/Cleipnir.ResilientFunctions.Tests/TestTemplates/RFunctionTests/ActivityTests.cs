using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class ActivityTests
{
    public abstract Task SunshineActionTest();
    public async Task SunshineActionTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var rFunctions = new RFunctions(store);
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var syncedCounter = new SyncedCounter();
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            async Task(string param, Context context) =>
            {
                var (activity, _) = context;
                await activity.Do(
                    id: "Test",
                    work: () => syncedCounter.Increment()
                );
            });

        await rAction.Schedule(functionInstanceId.ToString(), "hello");

        await BusyWait.Until(() =>
            store.GetFunction(functionId).SelectAsync(sf => sf?.Status == Status.Succeeded)
        );
        
        syncedCounter.Current.ShouldBe(1);
        var activityResults = await store.ActivityStore.GetActivityResults(functionId);
        activityResults.Single(r => r.ActivityId == "Test").WorkStatus.ShouldBe(WorkStatus.Completed);

        var controlPanel = await rAction.ControlPanels.For(functionId.InstanceId);
        controlPanel.ShouldNotBeNull();
        await controlPanel.ReInvoke();
        
        activityResults = await store.ActivityStore.GetActivityResults(functionId);
        activityResults.Single(r => r.ActivityId == "Test").WorkStatus.ShouldBe(WorkStatus.Completed);
        syncedCounter.Current.ShouldBe(1);
    }
    
    public abstract Task SunshineAsyncActionTest();
    public async Task SunshineAsyncActionTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var rFunctions = new RFunctions(store);
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var syncedCounter = new SyncedCounter();
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            async Task(string param, Context context) =>
            {
                var (activity, _) = context;
                await activity.Do(
                    id: "Test",
                    work: () => { syncedCounter.Increment(); return Task.CompletedTask; });
            });

        await rAction.Schedule(functionInstanceId.ToString(), "hello");

        await BusyWait.Until(() =>
            store.GetFunction(functionId).SelectAsync(sf => sf?.Status == Status.Succeeded)
        );
        
        syncedCounter.Current.ShouldBe(1);
        var activityResults = await store.ActivityStore.GetActivityResults(functionId);
        activityResults.Single(r => r.ActivityId == "Test").WorkStatus.ShouldBe(WorkStatus.Completed);

        var controlPanel = await rAction.ControlPanels.For(functionId.InstanceId);
        controlPanel.ShouldNotBeNull();
        await controlPanel.ReInvoke();
        
        activityResults = await store.ActivityStore.GetActivityResults(functionId);
        activityResults.Single(r => r.ActivityId == "Test").WorkStatus.ShouldBe(WorkStatus.Completed);
        syncedCounter.Current.ShouldBe(1);
    }
    
    public abstract Task SunshineFuncTest();
    public async Task SunshineFuncTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var rFunctions = new RFunctions(store);
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var syncedCounter = new SyncedCounter();
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            async Task(string param, Context context) =>
            {
                var (activity, _) = context;
                await activity.Do(
                    id: "Test",
                    work: () =>
                    {
                        syncedCounter.Increment();
                        return param;
                    });
            });

        await rAction.Schedule(functionInstanceId.ToString(), param: "hello");

        await BusyWait.Until(() =>
            store.GetFunction(functionId).SelectAsync(sf => sf?.Status == Status.Succeeded)
        );
        
        syncedCounter.Current.ShouldBe(1);
        var activityResults = await store.ActivityStore.GetActivityResults(functionId);
        var storedActivity = activityResults.Single(r => r.ActivityId == "Test");
        storedActivity.WorkStatus.ShouldBe(WorkStatus.Completed);
        storedActivity.Result!.DeserializeFromJsonTo<string>().ShouldBe("hello");

        var controlPanel = await rAction.ControlPanels.For(functionId.InstanceId);
        controlPanel.ShouldNotBeNull();
        await controlPanel.ReInvoke();
        
        activityResults = await store.ActivityStore.GetActivityResults(functionId);
        storedActivity = activityResults.Single(r => r.ActivityId == "Test");
        storedActivity.WorkStatus.ShouldBe(WorkStatus.Completed);
        storedActivity.Result!.DeserializeFromJsonTo<string>().ShouldBe("hello");
        syncedCounter.Current.ShouldBe(1);
    }
    
    public abstract Task SunshineAsyncFuncTest();
    public async Task SunshineAsyncFuncTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var rFunctions = new RFunctions(store);
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var syncedCounter = new SyncedCounter();
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            async Task(string param, Context context) =>
            {
                var (activity, _) = context;
                await activity.Do(
                    id: "Test",
                    work: () =>
                    {
                        syncedCounter.Increment();
                        return param.ToTask();
                    });
            });

        await rAction.Schedule(functionInstanceId.ToString(), param: "hello");

        await BusyWait.Until(() =>
            store.GetFunction(functionId).SelectAsync(sf => sf?.Status == Status.Succeeded)
        );
        
        syncedCounter.Current.ShouldBe(1);
        var activityResults = await store.ActivityStore.GetActivityResults(functionId);
        var storedActivity = activityResults.Single(r => r.ActivityId == "Test");
        storedActivity.WorkStatus.ShouldBe(WorkStatus.Completed);
        storedActivity.Result!.DeserializeFromJsonTo<string>().ShouldBe("hello");

        var controlPanel = await rAction.ControlPanels.For(functionId.InstanceId);
        controlPanel.ShouldNotBeNull();
        await controlPanel.ReInvoke();
        
        activityResults = await store.ActivityStore.GetActivityResults(functionId);
        storedActivity = activityResults.Single(r => r.ActivityId == "Test");
        storedActivity.WorkStatus.ShouldBe(WorkStatus.Completed);
        storedActivity.Result!.DeserializeFromJsonTo<string>().ShouldBe("hello");
        syncedCounter.Current.ShouldBe(1);
    }
    
    public abstract Task ExceptionThrowingActionTest();
    public async Task ExceptionThrowingActionTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var rFunctions = new RFunctions(store);
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var syncedCounter = new SyncedCounter();
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            async Task(string param, Context context) =>
            {
                var (activity, _) = context;
                await activity.Do(
                    id: "Test",
                    work: () =>
                    {
                        syncedCounter.Increment();
                        throw new InvalidOperationException("oh no");
                    });
            });

        await rAction.Schedule(functionInstanceId.ToString(), "hello");

        await BusyWait.Until(() =>
            store.GetFunction(functionId).SelectAsync(sf => sf?.Status == Status.Failed)
        );
        
        syncedCounter.Current.ShouldBe(1);
        var activityResults = await store.ActivityStore.GetActivityResults(functionId);
        var storedActivity = activityResults.Single(r => r.ActivityId == "Test");
        storedActivity.WorkStatus.ShouldBe(WorkStatus.Failed);
        storedActivity.StoredException.ShouldNotBeNull();
        storedActivity.StoredException.ExceptionType.ShouldContain("InvalidOperationException");

        var controlPanel = await rAction.ControlPanels.For(functionId.InstanceId);
        controlPanel.ShouldNotBeNull();
        await Should.ThrowAsync<ActivityException>(() => controlPanel.ReInvoke());
        
        activityResults = await store.ActivityStore.GetActivityResults(functionId);
        storedActivity = activityResults.Single(r => r.ActivityId == "Test");
        storedActivity.WorkStatus.ShouldBe(WorkStatus.Failed);
        storedActivity.StoredException.ShouldNotBeNull();
        storedActivity.StoredException.ExceptionType.ShouldContain("InvalidOperationException");
        syncedCounter.Current.ShouldBe(1);
    }
}