using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IMigrator
{
    Task MigrateToLatestSchema();
}