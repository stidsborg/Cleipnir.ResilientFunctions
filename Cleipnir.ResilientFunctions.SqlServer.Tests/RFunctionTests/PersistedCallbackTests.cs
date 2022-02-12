using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.RFunctionTests;

[Ignore] //todo remove
[TestClass]
public class PersistedCallbackTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.PersistedCallbackTests
{
    [TestMethod]
    public override Task PersistedCallbackIsInvokedAfterFuncStateHasBeenPersisted()
        => PersistedCallbackIsInvokedAfterFuncStateHasBeenPersisted(Sql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task PersistedCallbackIsInvokedAfterFuncWithScrapbookStateHasBeenPersisted()
        => PersistedCallbackIsInvokedAfterFuncWithScrapbookStateHasBeenPersisted(Sql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task PersistedCallbackIsInvokedAfterActionWithScrapbookStateHasBeenPersisted()
        => PersistedCallbackIsInvokedAfterActionWithScrapbookStateHasBeenPersisted(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task PersistedCallbackIsInvokedAfterActionStateHasBeenPersisted()
        => PersistedCallbackIsInvokedAfterActionStateHasBeenPersisted(Sql.AutoCreateAndInitializeStore());
}