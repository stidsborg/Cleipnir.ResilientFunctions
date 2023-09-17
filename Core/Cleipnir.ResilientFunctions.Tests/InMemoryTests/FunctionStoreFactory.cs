using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

public static class FunctionStoreFactory
{
    public static Task<IFunctionStore> Create()
        => new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask();
}