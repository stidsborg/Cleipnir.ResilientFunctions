using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Utils.Arbitrator;

public interface IArbitrator
{
    public Task<bool> Propose(string groupId, string instanceId, string value);
    public Task<bool> Propose(string groupId, string value);
}