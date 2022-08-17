using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

public static class Utils
{
    public static Task<IFunctionStore> CreateInMemoryFunctionStoreTask() 
        => new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask();
}