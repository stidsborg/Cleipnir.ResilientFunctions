using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class PostponedTests : ResilientFunctions.Tests.RFunctionTests.PostponedTests
{
    [TestMethod]
    public override Task PostponedFuncIsCompletedByWatchDog()
        => PostponedFuncIsCompletedByWatchDog(new InMemoryFunctionStore());

    [TestMethod]
    public override Task PostponedFuncWithScrapbookIsCompletedByWatchDog()
        => PostponedFuncWithScrapbookIsCompletedByWatchDog(new InMemoryFunctionStore());

    [TestMethod]
    public override Task PostponedActionIsCompletedByWatchDog()
        => PostponedActionIsCompletedByWatchDog(new InMemoryFunctionStore());

    [TestMethod]
    public override Task PostponedActionWithScrapbookIsCompletedByWatchDog()
        => PostponedActionWithScrapbookIsCompletedByWatchDog(new InMemoryFunctionStore());
}