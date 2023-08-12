using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage;

public interface ITimeoutStore
{
    Task Initialize();
    Task UpsertTimeout(StoredTimeout storedTimeout, bool overwrite);
    Task RemoveTimeout(FunctionId functionId, string timeoutId);
    Task<IEnumerable<StoredTimeout>> GetTimeouts(string functionTypeId, long expiresBefore);
}

public record StoredTimeout(FunctionId FunctionId, string TimeoutId, long Expiry);