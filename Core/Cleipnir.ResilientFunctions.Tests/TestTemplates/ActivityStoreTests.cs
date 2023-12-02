using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates;

public abstract class ActivityStoreTests
{
    public abstract Task SunshineScenarioTest();
    protected async Task SunshineScenarioTest(Task<IActivityStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var activity1 = new StoredActivity(
            "ActivityId1",
            WorkStatus.Started,
            Result: null,
            StoredException: null
        );
        var activity2 = new StoredActivity(
            "ActivityId2",
            WorkStatus.Completed,
            Result: null,
            StoredException: null
        );
        await store
            .GetActivityResults(functionId)
            .ToListAsync()
            .SelectAsync(l => l.Any())
            .ShouldBeFalseAsync();
        
        await store.SetActivityResult(functionId, activity1);
        
        var storedActivities = await store
            .GetActivityResults(functionId)
            .ToListAsync();
        storedActivities.Count.ShouldBe(1);
        var storedActivity1 = storedActivities[0];
        storedActivity1.ShouldBe(activity1);
        
        await store.SetActivityResult(functionId, activity2);
        storedActivities = await store.GetActivityResults(functionId).ToListAsync();
        storedActivities.Count.ShouldBe(2);
        storedActivities[0].ShouldBe(activity1);
        storedActivities[1].ShouldBe(activity2);
        
        await store.SetActivityResult(functionId, activity2);
        await store.GetActivityResults(functionId).ToListAsync();
        
        await store.SetActivityResult(functionId, activity2);
        storedActivities = await store.GetActivityResults(functionId).ToListAsync();
        storedActivities.Count.ShouldBe(2);
        storedActivities[0].ShouldBe(activity1);
        storedActivities[1].ShouldBe(activity2);
    }
    
    public abstract Task SingleActivityWithResultLifeCycle();
    protected async Task SingleActivityWithResultLifeCycle(Task<IActivityStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var activity = new StoredActivity(
            "ActivityId1",
            WorkStatus.Started,
            Result: null,
            StoredException: null
        );

        await store.GetActivityResults(functionId)
            .SelectAsync(r => r.Any())
            .ShouldBeFalseAsync();
        
        await store.SetActivityResult(functionId, activity);
        var storedActivity = await store.GetActivityResults(functionId).SelectAsync(r => r.Single());
        storedActivity.ShouldBe(activity);

        activity = activity with { WorkStatus = WorkStatus.Completed, Result = "Hello World" };
        await store.SetActivityResult(functionId, activity);
        storedActivity = await store.GetActivityResults(functionId).SelectAsync(r => r.Single());
        storedActivity.ShouldBe(activity);
    }
    
    public abstract Task SingleFailingActivityLifeCycle();
    protected async Task SingleFailingActivityLifeCycle(Task<IActivityStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var storedException = new StoredException(
            "Some Exception Message",
            "SomeStackTrace",
            "Some Exception Type"
        );
        var activity = new StoredActivity(
            "ActivityId1",
            WorkStatus.Started,
            Result: null,
            StoredException: null
        );

        await store.GetActivityResults(functionId)
            .SelectAsync(r => r.Any())
            .ShouldBeFalseAsync();
        
        await store.SetActivityResult(functionId, activity);
        var storedActivity = await store.GetActivityResults(functionId).SelectAsync(r => r.Single());
        storedActivity.ShouldBe(activity);

        activity = activity with { WorkStatus = WorkStatus.Completed, StoredException = storedException };
        await store.SetActivityResult(functionId, activity);
        storedActivity = await store.GetActivityResults(functionId).SelectAsync(r => r.Single());
        storedActivity.ShouldBe(activity);
    }
    
    public abstract Task ActivitiesCanBeDeleted();
    protected async Task ActivitiesCanBeDeleted(Task<IActivityStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var activity1 = new StoredActivity(
            "ActivityId1",
            WorkStatus.Started,
            Result: null,
            StoredException: null
        );
        var activity2 = new StoredActivity(
            "ActivityId2",
            WorkStatus.Completed,
            Result: null,
            StoredException: null
        );
        await store.SetActivityResult(functionId, activity1);
        await store.SetActivityResult(functionId, activity2);

        await store
            .GetActivityResults(functionId)
            .SelectAsync(sas => sas.Count() == 2)
            .ShouldBeTrueAsync();

        await store.DeleteActivityResult(functionId, activity2.ActivityId);
        var activityResults = await store.GetActivityResults(functionId).ToListAsync();
        activityResults.Count.ShouldBe(1);
        activityResults[0].ActivityId.ShouldBe(activity1.ActivityId);

        await store.DeleteActivityResult(functionId, activity2.ActivityId);
        activityResults = await store.GetActivityResults(functionId).ToListAsync();
        activityResults.Count.ShouldBe(1);
        activityResults[0].ActivityId.ShouldBe(activity1.ActivityId);
        
        await store.DeleteActivityResult(functionId, activity1.ActivityId);
        await store
            .GetActivityResults(functionId)
            .SelectAsync(sas => sas.Any())
            .ShouldBeFalseAsync();
    }
}