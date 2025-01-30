using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Storage;

public class InMemoryMigrator : IMigrator
{
    public Task MigrateToLatestSchema() => Task.CompletedTask;
}