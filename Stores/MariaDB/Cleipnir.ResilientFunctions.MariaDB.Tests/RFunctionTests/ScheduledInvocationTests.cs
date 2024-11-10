using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MariaDb.Tests.RFunctionTests;

[TestClass]
public class ScheduledInvocationTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.ScheduledInvocationTests
{
    [TestMethod]
    public override Task ScheduledFunctionIsInvokedAfterFuncStateHasBeenPersisted()
        => ScheduledFunctionIsInvokedAfterFuncStateHasBeenPersisted(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ScheduledFunctionIsInvokedAfterFuncWithStateHasBeenPersisted()
        => ScheduledFunctionIsInvokedAfterFuncWithStateHasBeenPersisted(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ScheduledFunctionIsInvokedAfterActionWithStateHasBeenPersisted()
        => ScheduledFunctionIsInvokedAfterActionWithStateHasBeenPersisted(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ScheduledFunctionIsInvokedAfterActionStateHasBeenPersisted()
        => ScheduledFunctionIsInvokedAfterActionStateHasBeenPersisted(FunctionStoreFactory.Create());
}