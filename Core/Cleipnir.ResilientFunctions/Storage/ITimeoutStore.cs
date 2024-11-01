using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage;

public interface ITimeoutStore
{
    Task Initialize();
    Task Truncate();
    Task UpsertTimeout(StoredTimeout storedTimeout, bool overwrite);
    Task RemoveTimeout(StoredId storedId, string timeoutId);
    Task Remove(StoredId storedId);
    Task<IEnumerable<StoredTimeout>> GetTimeouts(long expiresBefore);
    Task<IEnumerable<StoredTimeout>> GetTimeouts(StoredId storedId);
}

public record StoredTimeout(StoredId StoredId, string TimeoutId, long Expiry);