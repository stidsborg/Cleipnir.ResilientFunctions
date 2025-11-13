using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Queuing;

public interface IQueue
{
    Task<T> Next<T>() where T : notnull;
}