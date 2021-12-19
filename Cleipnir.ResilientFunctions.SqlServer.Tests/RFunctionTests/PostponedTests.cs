using System;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.RFunctionTests;

[TestClass]
public class PostponedTests : ResilientFunctions.Tests.RFunctionTests.PostponedTests
{
    private SqlServerFunctionStore Store { get; } = new SqlServerFunctionStore(Sql.ConnFunc);

    [TestInitialize]
    public async Task SetUp()
    {
        await Store.Initialize();
        await Store.Truncate();
    }

    [TestMethod]
    public override Task PostponedFuncIsCompletedByWatchDog()
        => PostponedFuncIsCompletedByWatchDog(Store);

    [TestMethod]
    public override Task PostponedFuncWithScrapbookIsCompletedByWatchDog()
        => PostponedFuncWithScrapbookIsCompletedByWatchDog(Store);

    [TestMethod]
    public override Task PostponedActionIsCompletedByWatchDog()
        => PostponedActionIsCompletedByWatchDog(Store);

    [TestMethod]
    public override Task PostponedActionWithScrapbookIsCompletedByWatchDog()
        => PostponedActionWithScrapbookIsCompletedByWatchDog(Store);
}