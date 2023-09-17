using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Redis.Tests;

public static class FunctionStoreFactory
{
    public static Task<IFunctionStore> FunctionStoreTask { get; } 
        = new RedisFunctionStore("localhost").CastTo<IFunctionStore>().ToTask();
}