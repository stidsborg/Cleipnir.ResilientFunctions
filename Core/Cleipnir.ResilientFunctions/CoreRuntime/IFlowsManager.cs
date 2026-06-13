using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

/// <summary>
/// Schedules a stored flow for immediate execution so it consumes a just-published message.
/// Implemented by <see cref="FlowsManager"/>.
/// </summary>
public interface IFlowsManager
{
    Task Schedule(StoredId storedId);
}
