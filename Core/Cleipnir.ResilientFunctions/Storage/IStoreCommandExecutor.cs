using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IStoreCommandExecutor
{
    public Task<IStoreCommandReader> Execute(StoreCommands commands);
    Task<int> ExecuteNonQuery(StoreCommands commands);
}