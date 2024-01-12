using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public enum ResiliencyLevel
{
    AtLeastOnce,
    AtMostOnce
}

public class Activity
{
    private readonly Dictionary<string, StoredActivity> _activityResults;
    private readonly IActivityStore _activityStore;
    private readonly ISerializer _serializer;
    private readonly FunctionId _functionId;
    private readonly object _sync = new();
    
    public Activity(FunctionId functionId, IEnumerable<StoredActivity> existingActivities, IActivityStore activityStore, ISerializer serializer)
    {
        _functionId = functionId;
        _activityStore = activityStore;
        _serializer = serializer;

        _activityResults = existingActivities.ToDictionary(sa => sa.ActivityId, sa => sa);
    }

    public Task Do(string id, Action work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Do(id, work: () => { work(); return Task.CompletedTask; }, resiliency);
    public Task<T> Do<T>(string id, Func<T> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Do(id, work: () => work().ToTask(), resiliency);

    public async Task Do(string id, Func<Task> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
    {
        lock (_sync)
        {
            var success = _activityResults.TryGetValue(id, out var activityResult);
            if (success && activityResult!.WorkStatus == WorkStatus.Completed)
                return;
            if (success && activityResult!.WorkStatus == WorkStatus.Failed)
                throw new ActivityException(_functionId, id, _serializer.DeserializeException(activityResult.StoredException!));
            if (success && resiliency == ResiliencyLevel.AtMostOnce)
                throw new InvalidOperationException($"Activity '{id}' started but did not complete previously");
        }

        if (resiliency == ResiliencyLevel.AtMostOnce)
        {
            await _activityStore.SetActivityResult(
                _functionId,
                new StoredActivity(id, WorkStatus.Started, Result: null, StoredException: null)
            );
            lock (_sync)
                _activityResults[id] = new StoredActivity(id, WorkStatus.Started, Result: null, StoredException: null);
        }
        
        try
        {
            await work();
        }
        catch (PostponeInvocationException)
        {
            throw;
        }
        catch (SuspendInvocationException)
        {
            throw;
        }
        catch (Exception exception)
        {
            var storedException = _serializer.SerializeException(exception);
            var storedActivity = new StoredActivity(id, WorkStatus.Failed, Result: null, storedException);
            await _activityStore.SetActivityResult(_functionId, storedActivity);
            
            lock (_sync)
                _activityResults[id] = new StoredActivity(id, WorkStatus.Failed, Result: null, StoredException: storedException);

            throw;
        }

        await _activityStore.SetActivityResult(
            _functionId,
            new StoredActivity(id, WorkStatus.Completed, Result: null, StoredException: null)
        );
    }
    
    public async Task<T> Do<T>(string id, Func<Task<T>> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
    {
        lock (_sync)
        {
            var success = _activityResults.TryGetValue(id, out var activityResult);
            if (success && activityResult!.WorkStatus == WorkStatus.Completed)
                return (activityResult.Result == null ? default : JsonSerializer.Deserialize<T>(activityResult.Result))!;
            if (success && activityResult!.WorkStatus == WorkStatus.Failed)
                throw new PreviousFunctionInvocationException(_functionId, _serializer.DeserializeException(activityResult.StoredException!));
            if (success && resiliency == ResiliencyLevel.AtMostOnce)
                throw new InvalidOperationException($"Activity '{id}' started but did not complete previously");
        }

        if (resiliency == ResiliencyLevel.AtMostOnce)
        {
            await _activityStore.SetActivityResult(
                _functionId,
                new StoredActivity(id, WorkStatus.Started, Result: null, StoredException: null)
            );
            lock (_sync)
                _activityResults[id] = new StoredActivity(id, WorkStatus.Started, Result: null, StoredException: null);
        }

        T result;
        try
        {
            result = await work();
        }
        catch (PostponeInvocationException)
        {
            throw;
        }
        catch (SuspendInvocationException)
        {
            throw;
        }
        catch (Exception exception)
        {
            var storedException = _serializer.SerializeException(exception);
            var storedActivity = new StoredActivity(id, WorkStatus.Failed, Result: null, storedException);
            await _activityStore.SetActivityResult(_functionId, storedActivity);
            
            lock (_sync)
                _activityResults[id] = new StoredActivity(id, WorkStatus.Failed, Result: null, StoredException: storedException);

            throw;
        }

        await _activityStore.SetActivityResult(
            _functionId,
            new StoredActivity(id, WorkStatus.Completed, Result: _serializer.SerializeActivityResult(result), StoredException: null)
        );

        return result;
    }
    
    public Task<T> WhenAny<T>(string id, params Task<T>[] tasks)
        => Do(id, async () => await await Task.WhenAny(tasks));
}