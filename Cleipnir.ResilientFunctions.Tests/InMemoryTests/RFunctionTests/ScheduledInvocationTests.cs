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
            new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask()
        );

    [TestMethod]
    public override Task ScheduledFunctionIsInvokedAfterFuncWithScrapbookStateHasBeenPersisted()
        => ScheduledFunctionIsInvokedAfterFuncWithScrapbookStateHasBeenPersisted(
            new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask()
        );

    [TestMethod]
    public override Task ScheduledFunctionIsInvokedAfterActionWithScrapbookStateHasBeenPersisted()
        => ScheduledFunctionIsInvokedAfterActionWithScrapbookStateHasBeenPersisted(
            new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask()
        );

    [TestMethod]
    public override Task ScheduledFunctionIsInvokedAfterActionStateHasBeenPersisted()
        => ScheduledFunctionIsInvokedAfterActionStateHasBeenPersisted(
            new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask()
        );
}