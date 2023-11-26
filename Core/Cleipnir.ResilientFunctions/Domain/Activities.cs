using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Domain;

public enum ResiliencyLevel
{
    AtLeastOnce,
    AtMostOnce
}

public class Activities
{
    private readonly Dictionary<string, ActivityResult> _activityResults = new();
    private readonly object _sync = new();
    
    public async Task Do(string id, Action work, ResiliencyLevel resiliencyLevel = ResiliencyLevel.AtLeastOnce)
    {
        lock (_sync)
        {
            var success = _activityResults.TryGetValue(id, out var activityResult);
            if (success && activityResult!.WorkStatus == WorkStatus.Completed)
                return;
            if (success && resiliencyLevel == ResiliencyLevel.AtMostOnce)
                throw new InvalidOperationException($"Activity '{id}' started but not completed previously");
            work();
            

        }
            
        throw new NotImplementedException();
    }
    
    public async Task Do<T>(string id, Func<T> work, ResiliencyLevel resiliencyLevel = ResiliencyLevel.AtLeastOnce)
    {
        throw new NotImplementedException();
    }

    public async Task Do(string id, Func<Task> work, ResiliencyLevel resiliencyLevel = ResiliencyLevel.AtLeastOnce)
    {
        throw new NotImplementedException();
    }
    
    public async Task Do<T>(string id, Func<Task<T>> work, ResiliencyLevel resiliencyLevel = ResiliencyLevel.AtLeastOnce)
    {
        throw new NotImplementedException();
    } 
}

public record ActivityResult(WorkStatus WorkStatus, string? Result, Exception? ThrownException);