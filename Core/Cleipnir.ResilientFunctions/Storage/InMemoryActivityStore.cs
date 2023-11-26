using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Storage;

public class InMemoryActivityStore : IActivityStore
{
    private readonly Dictionary<FunctionId, Dictionary<string, StoredActivity>> _activities = new();
    private readonly object _sync = new();

    public Task Initialize() => Task.CompletedTask;

    public Task SetActivityResult(FunctionId functionId, StoredActivity storedActivity)
    {
        lock (_sync)
        {
            if (!_activities.ContainsKey(functionId))
                _activities[functionId] = new Dictionary<string, StoredActivity>();
                
            _activities[functionId][storedActivity.ActivityId] = storedActivity;
        }
        
        return Task.CompletedTask;
    }

    public Task<IEnumerable<StoredActivity>> GetActivityResults(FunctionId functionId)
    {
        lock (_sync)
            return !_activities.ContainsKey(functionId)
                ? Enumerable.Empty<StoredActivity>().ToTask()
                : _activities[functionId].Values.ToList().AsEnumerable().ToTask();
    }
}