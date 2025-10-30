using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IStoreCommandExecutor
{
    public Task<IStoreCommandReader> Execute(StoreCommands commands);
    public Task<IStoreCommandReader> Execute(StoreCommand command);
    Task<int> ExecuteNonQuery(StoreCommands commands);
    Task<int> ExecuteNonQuery(StoreCommand commands);
}