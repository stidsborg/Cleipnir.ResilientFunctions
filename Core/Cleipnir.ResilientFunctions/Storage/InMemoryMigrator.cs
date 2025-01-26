using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Storage;

public class InMemoryMigrator : IMigrator
{
    public Task<int?> Initialize(int version) => default(int?).ToTask();
    public Task SetVersion(int version) => Task.CompletedTask;
}