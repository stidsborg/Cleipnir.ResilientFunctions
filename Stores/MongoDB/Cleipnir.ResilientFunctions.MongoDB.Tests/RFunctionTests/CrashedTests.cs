using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MongoDB.Tests.RFunctionTests;

[TestClass]
public class CrashedTests : ResilientFunctions.Tests.TestTemplates.RFunctionTests.CrashedTests
{
    [TestMethod]
    public override Task NonCompletedFuncIsCompletedByWatchDog()
        => NonCompletedFuncIsCompletedByWatchDog(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task NonCompletedFuncWithScrapbookIsCompletedByWatchDog()
        => NonCompletedFuncWithScrapbookIsCompletedByWatchDog(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task NonCompletedActionIsCompletedByWatchDog()
        => NonCompletedActionIsCompletedByWatchDog(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task NonCompletedActionWithScrapbookIsCompletedByWatchDog()
        => NonCompletedActionWithScrapbookIsCompletedByWatchDog(NoSql.AutoCreateAndInitializeStore());
}