namespace Cleipnir.ResilientFunctions.MongoDB.Tests.RFunctionTests;

using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

[TestClass]
public class CrashedMethodTests : ResilientFunctions.Tests.TestTemplates.RFunctionTests.CrashedMethodTests
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