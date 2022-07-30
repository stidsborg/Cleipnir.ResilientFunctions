﻿using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.StressTests.Engines;

public interface IEngine
{
    Task RecreateDatabase();
    Task InitializeDatabaseAndInitializeAndTruncateTable();
    Task<int> NumberOfNonCompleted();
    Task<int> NumberOfSuccessfullyCompleted();
    Task<IFunctionStore> CreateFunctionStore();
}