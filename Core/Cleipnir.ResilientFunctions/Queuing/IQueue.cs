using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Queuing;

public interface IQueue
{
    Task<T?> Peek<T>() where T : notnull;
    Task<T> SuspendUntilPeek<T>() where T : notnull;
}