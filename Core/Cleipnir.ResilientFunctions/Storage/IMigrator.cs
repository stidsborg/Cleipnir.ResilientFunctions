using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IMigrator
{
    Task<int?> Initialize(int version);
    Task SetVersion(int version);
}