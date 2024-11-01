using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage;

public interface ICorrelationStore
{
    public Task Initialize();
    public Task Truncate();

    public Task SetCorrelation(StoredId storedId, string correlationId);
    public Task<IReadOnlyList<StoredId>> GetCorrelations(string correlationId);
    public Task<IReadOnlyList<string>> GetCorrelations(StoredType flowType, string correlationId);
    public Task<IReadOnlyList<string>> GetCorrelations(StoredId storedId);
    public Task RemoveCorrelations(StoredId storedId);
    public Task RemoveCorrelation(StoredId storedId, string correlationId);
}