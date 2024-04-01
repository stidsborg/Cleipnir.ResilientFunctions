using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage;

public interface ITimeoutStore
{
    Task Initialize();
    Task Truncate();
    Task UpsertTimeout(StoredTimeout storedTimeout, bool overwrite);
    Task RemoveTimeout(FunctionId functionId, string timeoutId);
    Task Remove(FunctionId functionId);
    Task<IEnumerable<StoredTimeout>> GetTimeouts(string functionTypeId, long expiresBefore);
    Task<IEnumerable<StoredTimeout>> GetTimeouts(FunctionId functionId);
}

public record StoredTimeout(FunctionId FunctionId, string TimeoutId, long Expiry);