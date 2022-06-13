using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.StressTests.Common.Engines;

public interface IEngine
{
    Task InitializeDatabaseAndInitializeAndTruncateTable();
    Task<int> NumberOfNonCompleted();
    Task<int> NumberOfSuccessfullyCompleted();
    Task<IFunctionStore> CreateFunctionStore();
}