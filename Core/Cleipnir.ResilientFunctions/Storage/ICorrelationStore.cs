using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage;

public interface ICorrelationStore
{
    public Task Initialize();
    public Task Truncate();

    public Task SetCorrelation(FunctionId functionId, string correlationId);
    public Task<IReadOnlyList<FunctionId>> GetCorrelations(string correlationId);
    public Task<IReadOnlyList<string>> GetCorrelations(FunctionId functionId);
    public Task RemoveCorrelations(FunctionId functionId);
    public Task RemoveCorrelation(FunctionId functionId, string correlationId);
}