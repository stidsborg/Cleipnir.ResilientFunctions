using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[Ignore] //todo remove
[TestClass]
public class PersistedCallbackTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.PersistedCallbackTests
{
    [TestMethod]
    public override Task PersistedCallbackIsInvokedAfterFuncStateHasBeenPersisted()
        => PersistedCallbackIsInvokedAfterFuncStateHasBeenPersisted(new InMemoryFunctionStore());
    
    [TestMethod]
    public override Task PersistedCallbackIsInvokedAfterFuncWithScrapbookStateHasBeenPersisted()
        => PersistedCallbackIsInvokedAfterFuncWithScrapbookStateHasBeenPersisted(new InMemoryFunctionStore());
    
    [TestMethod]
    public override Task PersistedCallbackIsInvokedAfterActionWithScrapbookStateHasBeenPersisted()
        => PersistedCallbackIsInvokedAfterActionWithScrapbookStateHasBeenPersisted(new InMemoryFunctionStore());

    [TestMethod]
    public override Task PersistedCallbackIsInvokedAfterActionStateHasBeenPersisted()
        => PersistedCallbackIsInvokedAfterActionStateHasBeenPersisted(new InMemoryFunctionStore());
}