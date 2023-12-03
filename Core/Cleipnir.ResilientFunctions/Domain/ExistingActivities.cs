using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class ExistingActivities
{
    private readonly FunctionId _functionId;
    private readonly Dictionary<string, StoredActivity> _storedActivities;
    private readonly IActivityStore _activityStore;

    public ExistingActivities(FunctionId functionId, Dictionary<string, StoredActivity> storedActivities, IActivityStore activityStore)
    {
        _functionId = functionId;
        _storedActivities = storedActivities;
        _activityStore = activityStore;
    }

    public IReadOnlyDictionary<string, StoredActivity> All => _storedActivities;

    public async Task Remove(string id)
    {
        await _activityStore.DeleteActivityResult(_functionId, id);
        _storedActivities.Remove(id);
    }

    public async Task Set(StoredActivity storedActivity) //todo set result, set error, set started
    {
        await _activityStore.SetActivityResult(_functionId, storedActivity);
        _storedActivities[storedActivity.ActivityId] = storedActivity;
    }
}