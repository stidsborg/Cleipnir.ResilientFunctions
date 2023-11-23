using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IActivityStore
{
    Task SetActivityResult(FunctionId functionId, StoredActivity storedActivity);
    Task<IEnumerable<StoredActivity>> GetActivityResults(FunctionId functionId);
}