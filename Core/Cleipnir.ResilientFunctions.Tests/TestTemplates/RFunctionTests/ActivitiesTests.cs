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

public abstract class ActivitiesTests
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

        var controlPanel = await rAction.ControlPanel(functionId.InstanceId);
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

        var controlPanel = await rAction.ControlPanel(functionId.InstanceId);
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

        var controlPanel = await rAction.ControlPanel(functionId.InstanceId);
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

        var controlPanel = await rAction.ControlPanel(functionId.InstanceId);
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

        var controlPanel = await rAction.ControlPanel(functionId.InstanceId);
        controlPanel.ShouldNotBeNull();
        await Should.ThrowAsync<ActivityException>(() => controlPanel.ReInvoke());
        
        activityResults = await store.ActivityStore.GetActivityResults(functionId);
        storedActivity = activityResults.Single(r => r.ActivityId == "Test");
        storedActivity.WorkStatus.ShouldBe(WorkStatus.Failed);
        storedActivity.StoredException.ShouldNotBeNull();
        storedActivity.StoredException.ExceptionType.ShouldContain("InvalidOperationException");
        syncedCounter.Current.ShouldBe(1);
    }
    
    public abstract Task TaskWhenAnyFuncTest();
    public async Task TaskWhenAnyFuncTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var rFunctions = new RFunctions(store);
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var rAction = rFunctions.RegisterFunc(
            functionTypeId,
            async Task<int> (string param, Context context) =>
            {
                var (activity, _) = context;
                var t1 = new Task<int>(() => 1);
                var t2 = Task.FromResult(2);
                return await activity.WhenAny("WhenAny", t1, t2);
            });

        var result = await rAction.Invoke(functionInstanceId.ToString(), param: "hello");
        result.ShouldBe(2);
        
        var activityResults = await store.ActivityStore.GetActivityResults(functionId);
        var storedActivity = activityResults.Single(r => r.ActivityId == "WhenAny");
        storedActivity.WorkStatus.ShouldBe(WorkStatus.Completed);
        storedActivity.Result!.DeserializeFromJsonTo<int>().ShouldBe(2);
    }
    
    public abstract Task ClearActivityTest();
    public async Task ClearActivityTest(Task<IFunctionStore> storeTask)
    {  
        var store = await storeTask;
        using var rFunctions = new RFunctions(store);
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;

        await store.CreateFunction(
            functionId,
            Test.SimpleStoredParameter,
            Test.SimpleStoredScrapbook,
            leaseExpiration: (DateTime.Now + TimeSpan.FromMinutes(1)).Ticks,
            postponeUntil: null,
            timestamp: DateTime.Now.Ticks
        );
        
        var registration = rFunctions.RegisterAction(
            functionTypeId,
            async Task (string param, Context context) =>
            {
                var (activities, _) = context;
                await activities.Clear("SomeActivity");
            });

        var controlPanel = await registration.ControlPanel(functionId.InstanceId);
        controlPanel.ShouldNotBeNull();

        await controlPanel.Activities.SetSucceeded("SomeActivity");
        await controlPanel.ReInvoke();

        await controlPanel.Refresh();
        controlPanel.Activities.All.Count.ShouldBe(0);
    }
}