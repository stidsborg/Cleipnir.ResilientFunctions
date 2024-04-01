using System.Runtime.CompilerServices;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.MySQL.Tests;

public static class FunctionStoreFactory
{
    public static Task<IFunctionStore> Create([CallerFilePath] string sourceFilePath = "", [CallerMemberName] string callMemberName = "") 
        => Sql.AutoCreateAndInitializeStore(sourceFilePath, callMemberName);

    public static Task<IEffectsStore> CreateEffectStore([CallerFilePath] string sourceFilePath = "", [CallerMemberName] string callMemberName = "")
        => Create(sourceFilePath, callMemberName).SelectAsync(fs => fs.EffectsStore);
}