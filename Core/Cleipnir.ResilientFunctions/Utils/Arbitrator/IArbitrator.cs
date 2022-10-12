using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Utils.Arbitrator;

public interface IArbitrator
{
    public Task<bool> Propose(string groupId, string instanceId, string value);
    public Task<bool> Propose(string groupId, string value);
    public Task Delete(string groupId);
    public Task Delete(string groupId, string instanceId);
}