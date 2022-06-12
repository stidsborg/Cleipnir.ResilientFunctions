using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.StressTests;

public interface IHelper
{
    Task InitializeDatabaseAndInitializeAndTruncateTable();
    Task<int> NumberOfNonCompleted();
    Task<int> NumberOfSuccessfullyCompleted();
    Task<IFunctionStore> CreateFunctionStore();
}