using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage;

public interface ITimeoutStore
{
    Task Initialize();
    Task Truncate();
    Task UpsertTimeout(StoredTimeout storedTimeout, bool overwrite);
    Task RemoveTimeout(FlowId flowId, string timeoutId);
    Task Remove(FlowId flowId);
    Task<IEnumerable<StoredTimeout>> GetTimeouts(string functionTypeId, long expiresBefore);
    Task<IEnumerable<StoredTimeout>> GetTimeouts(FlowId flowId);
}

public record StoredTimeout(FlowId FlowId, string TimeoutId, long Expiry);