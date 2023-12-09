using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class ExistingActivities
{
    private readonly FunctionId _functionId;
    private readonly Dictionary<string, StoredActivity> _storedActivities;
    private readonly IActivityStore _activityStore;
    private readonly ISerializer _serializer;

    public ExistingActivities(FunctionId functionId, Dictionary<string, StoredActivity> storedActivities, IActivityStore activityStore, ISerializer serializer)
    {
        _functionId = functionId;
        _storedActivities = storedActivities;
        _activityStore = activityStore;
        _serializer = serializer;
    }

    public IReadOnlyDictionary<string, StoredActivity> All => _storedActivities;

    public bool HasValue(string id) => _storedActivities.ContainsKey(id);
    public TResult? GetValue<TResult>(string id)
    {
        var success = _storedActivities.TryGetValue(id, out var storedActivity);
        if (!success)
            throw new KeyNotFoundException($"Activity '{id}' was not found in existing activities");
        if (storedActivity!.WorkStatus != WorkStatus.Completed)
            throw new InvalidOperationException($"Activity '{id}' has not completed (but has status '{storedActivity.WorkStatus}')");

        return storedActivity.Result == null 
            ? default 
            : _serializer.DeserializeActivityResult<TResult>(_storedActivities[id].Result!);
    } 
        
            
    
    public async Task Remove(string id)
    {
        await _activityStore.DeleteActivityResult(_functionId, id);
        _storedActivities.Remove(id);
    }

    private async Task Set(StoredActivity storedActivity) 
    {
        await _activityStore.SetActivityResult(_functionId, storedActivity);
        _storedActivities[storedActivity.ActivityId] = storedActivity;
    }

    public Task SetStarted(string activityId) 
        => Set(new StoredActivity(activityId, WorkStatus.Started, Result: null, StoredException: null));
    
    public Task SetSucceeded(string activityId)
        => Set(new StoredActivity(activityId, WorkStatus.Completed, Result: null, StoredException: null));
    
    public Task SetSucceeded<TResult>(string activityId, TResult result)
        => Set(new StoredActivity(activityId, WorkStatus.Completed, Result: _serializer.SerializeActivityResult(result), StoredException: null));

    public Task SetFailed(string activityId, Exception exception)
        => Set(new StoredActivity(activityId, WorkStatus.Failed, Result: null, StoredException: _serializer.SerializeException(exception)));
}