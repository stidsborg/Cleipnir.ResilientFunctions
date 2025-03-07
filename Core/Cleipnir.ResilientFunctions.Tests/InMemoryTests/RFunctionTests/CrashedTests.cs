using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class CrashedTests : TestTemplates.FunctionTests.CrashedTests
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