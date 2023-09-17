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
    public override Task ScheduledFunctionIsInvokedAfterFuncWithScrapbookStateHasBeenPersisted()
        => ScheduledFunctionIsInvokedAfterFuncWithScrapbookStateHasBeenPersisted(
            FunctionStoreFactory.Create()
        );

    [TestMethod]
    public override Task ScheduledFunctionIsInvokedAfterActionWithScrapbookStateHasBeenPersisted()
        => ScheduledFunctionIsInvokedAfterActionWithScrapbookStateHasBeenPersisted(
            FunctionStoreFactory.Create()
        );

    [TestMethod]
    public override Task ScheduledFunctionIsInvokedAfterActionStateHasBeenPersisted()
        => ScheduledFunctionIsInvokedAfterActionStateHasBeenPersisted(
            FunctionStoreFactory.Create()
        );
}