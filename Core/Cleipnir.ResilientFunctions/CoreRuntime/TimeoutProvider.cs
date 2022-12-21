using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public class TimeoutProvider
{
    private readonly ITimeoutStore _timeoutStore;
    private readonly FunctionId _functionId;

    public TimeoutProvider(ITimeoutStore timeoutStore, FunctionId functionId)
    {
        _timeoutStore = timeoutStore;
        _functionId = functionId;
    }

    public async Task RegisterTimeout(string timeoutId, DateTime expiry) 
        => await _timeoutStore.UpsertTimeout(new StoredTimeout(_functionId, timeoutId, expiry.ToUniversalTime().Ticks));

    public Task RegisterTimeout(string timeoutId, TimeSpan expiresIn)
        => RegisterTimeout(timeoutId, DateTime.UtcNow.Add(expiresIn));

    public Task CancelTimeout(string timeoutId) => _timeoutStore.RemoveTimeout(_functionId, timeoutId);
}