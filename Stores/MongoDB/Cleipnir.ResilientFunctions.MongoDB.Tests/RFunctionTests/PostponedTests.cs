using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MongoDB.Tests.RFunctionTests;

[TestClass]
public class PostponedTests : ResilientFunctions.Tests.TestTemplates.RFunctionTests.PostponedTests
{
    [TestMethod]
    public override Task PostponedFuncIsCompletedByWatchDog()
        => PostponedFuncIsCompletedByWatchDog(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task PostponedFuncWithScrapbookIsCompletedByWatchDog()
        => PostponedFuncWithScrapbookIsCompletedByWatchDog(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task PostponedActionIsCompletedByWatchDog()
        => PostponedActionIsCompletedByWatchDog(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task PostponedActionWithScrapbookIsCompletedByWatchDog()
        => PostponedActionWithScrapbookIsCompletedByWatchDog(NoSql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task PostponedActionIsCompletedAfterInMemoryTimeout()
        => PostponedActionIsCompletedAfterInMemoryTimeout(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task PostponedActionIsCompletedByWatchDogAfterCrash()
        => PostponedActionIsCompletedByWatchDogAfterCrash(NoSql.AutoCreateAndInitializeStore());
}