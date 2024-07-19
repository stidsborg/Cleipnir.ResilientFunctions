using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage;

public interface ICorrelationStore
{
    public Task Initialize();
    public Task Truncate();

    public Task SetCorrelation(FlowId flowId, string correlationId);
    public Task<IReadOnlyList<FlowId>> GetCorrelations(string correlationId);
    public Task<IReadOnlyList<FlowInstance>> GetCorrelations(FlowType flowType, string correlationId);
    public Task<IReadOnlyList<string>> GetCorrelations(FlowId flowId);
    public Task RemoveCorrelations(FlowId flowId);
    public Task RemoveCorrelation(FlowId flowId, string correlationId);
}