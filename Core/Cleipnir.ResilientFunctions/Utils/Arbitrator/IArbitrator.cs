using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Utils.Arbitrator;

public interface IArbitrator
{
    public Task<bool> Propose(string group, string name, string value);
    public Task Delete(string group, string name);
}