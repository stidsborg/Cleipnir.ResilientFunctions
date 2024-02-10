using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.AzureBlob.Tests.RFunctionTests;

[TestClass]
public class
    ScheduledInvocationTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.ScheduledInvocationTests
{
    [TestMethod]
    public override Task ScheduledFunctionIsInvokedAfterFuncStateHasBeenPersisted()
        => ScheduledFunctionIsInvokedAfterFuncStateHasBeenPersisted(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ScheduledFunctionIsInvokedAfterFuncWithStateHasBeenPersisted()
        => ScheduledFunctionIsInvokedAfterFuncWithStateHasBeenPersisted(FunctionStoreFactory
            .FunctionStoreTask);

    [TestMethod]
    public override Task ScheduledFunctionIsInvokedAfterActionWithStateHasBeenPersisted()
        => ScheduledFunctionIsInvokedAfterActionWithStateHasBeenPersisted(FunctionStoreFactory
            .FunctionStoreTask);

    [TestMethod]
    public override Task ScheduledFunctionIsInvokedAfterActionStateHasBeenPersisted()
        => ScheduledFunctionIsInvokedAfterActionStateHasBeenPersisted(FunctionStoreFactory.Create());
}