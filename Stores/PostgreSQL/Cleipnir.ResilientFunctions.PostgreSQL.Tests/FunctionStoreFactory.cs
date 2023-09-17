using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Tests;

public static class FunctionStoreFactory
{
    public static Task<IFunctionStore> Create([CallerFilePath] string sourceFilePath = "", [CallerMemberName] string callMemberName = "") 
        => Sql.AutoCreateAndInitializeStore(sourceFilePath, callMemberName);
}