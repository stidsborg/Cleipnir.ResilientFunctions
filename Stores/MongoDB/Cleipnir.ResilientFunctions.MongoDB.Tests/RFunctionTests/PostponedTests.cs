using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MongoDB.Tests.RFunctionTests;

[TestClass]
public class PostponedTests : ResilientFunctions.Tests.TestTemplates.RFunctionTests.PostponedTests
{
    [TestMethod]
    public override Task PostponedFuncIsCompletedByWatchDog()
        => PostponedFuncIsCompletedByWatchDog(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task PostponedFuncWithScrapbookIsCompletedByWatchDog()
        => PostponedFuncWithScrapbookIsCompletedByWatchDog(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task PostponedActionIsCompletedByWatchDog()
        => PostponedActionIsCompletedByWatchDog(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task PostponedActionWithScrapbookIsCompletedByWatchDog()
        => PostponedActionWithScrapbookIsCompletedByWatchDog(Sql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task PostponedActionIsCompletedAfterInMemoryTimeout()
        => PostponedActionIsCompletedAfterInMemoryTimeout(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task PostponedActionIsCompletedByWatchDogAfterCrash()
        => PostponedActionIsCompletedByWatchDogAfterCrash(Sql.AutoCreateAndInitializeStore());
}