using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class ScheduledInvocationTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.ScheduledInvocationTests
{
    [TestMethod]
    public override Task ScheduledFunctionIsInvokedAfterFuncStateHasBeenPersisted()
        => ScheduledFunctionIsInvokedAfterFuncStateHasBeenPersisted(
            FunctionStoreFactory.Create()
        );

    [TestMethod]
    public override Task ScheduledFunctionIsInvokedAfterFuncWithStateHasBeenPersisted()
        => ScheduledFunctionIsInvokedAfterFuncWithStateHasBeenPersisted(
            FunctionStoreFactory.Create()
        );

    [TestMethod]
    public override Task ScheduledFunctionIsInvokedAfterActionWithStateHasBeenPersisted()
        => ScheduledFunctionIsInvokedAfterActionWithStateHasBeenPersisted(
            FunctionStoreFactory.Create()
        );

    [TestMethod]
    public override Task ScheduledFunctionIsInvokedAfterActionStateHasBeenPersisted()
        => ScheduledFunctionIsInvokedAfterActionStateHasBeenPersisted(
            FunctionStoreFactory.Create()
        );
}