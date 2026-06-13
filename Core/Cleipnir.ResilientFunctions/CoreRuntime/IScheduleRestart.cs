using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

/// <summary>
/// Restarts a stored flow on this replica. Implemented by <see cref="Invocation.Invoker{TParam,TReturn}"/>.
/// </summary>
public interface IScheduleRestart
{
    Task ScheduleRestart(StoredId storedId);
}
