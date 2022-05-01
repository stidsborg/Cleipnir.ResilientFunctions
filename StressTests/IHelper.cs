using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.StressTests;

public interface IHelper
{
    Task InitializeDatabaseAndTruncateTable();
    Task<int> NumberOfNonCompleted();
    IFunctionStore CreateFunctionStore();
}