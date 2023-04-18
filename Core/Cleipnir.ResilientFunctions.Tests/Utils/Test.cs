using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

public static class Test
{
    public static StoredParameter SimpleStoredParameter { get; } = 
        new(ParamJson: "hello world", ParamType: typeof(string).SimpleQualifiedName());

    public static StoredScrapbook SimpleStoredScrapbook { get; }
        = new(ScrapbookJson: new RScrapbook().ToJson(), ScrapbookType: typeof(RScrapbook).SimpleQualifiedName());
}