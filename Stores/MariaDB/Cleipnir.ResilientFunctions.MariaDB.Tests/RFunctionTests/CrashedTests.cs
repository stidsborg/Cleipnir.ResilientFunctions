namespace Cleipnir.ResilientFunctions.MariaDb.Tests.RFunctionTests;

[TestClass]
public class CrashedTests : ResilientFunctions.Tests.TestTemplates.FunctionTests.CrashedTests
{
    [TestMethod]
    public override Task NonCompletedFuncIsCompletedByWatchDog()
        => NonCompletedFuncIsCompletedByWatchDog(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task NonCompletedActionIsCompletedByWatchDog()
        => NonCompletedActionIsCompletedByWatchDog(FunctionStoreFactory.Create());
}