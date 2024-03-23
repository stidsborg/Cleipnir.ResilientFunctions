using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

public static class Test
{
    public static StoredParameter SimpleStoredParameter { get; } = 
        new(ParamJson: "hello world".ToJson(), ParamType: typeof(string).SimpleQualifiedName());
}