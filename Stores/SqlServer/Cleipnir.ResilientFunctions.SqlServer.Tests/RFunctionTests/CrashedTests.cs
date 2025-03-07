using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.RFunctionTests;

[TestClass]
public class CrashedTests : ResilientFunctions.Tests.TestTemplates.FunctionTests.CrashedTests
{
    [TestMethod]
    public override Task NonCompletedFuncIsCompletedByWatchDog()
        => NonCompletedFuncIsCompletedByWatchDog(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task NonCompletedFuncWithStateIsCompletedByWatchDog()
        => NonCompletedFuncWithStateIsCompletedByWatchDog(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task NonCompletedActionIsCompletedByWatchDog()
        => NonCompletedActionIsCompletedByWatchDog(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task NonCompletedActionWithStateIsCompletedByWatchDog()
        => NonCompletedActionWithStateIsCompletedByWatchDog(FunctionStoreFactory.Create());
}