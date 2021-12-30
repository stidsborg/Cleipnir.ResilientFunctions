using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.RFunctionTests;

[TestClass]
public class PersistedCallbackTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.PersistedCallbackTests
{
    [TestMethod]
    public override Task PersistedCallbackIsInvokedAfterFuncStateHasBeenPersisted()
        => PersistedCallbackIsInvokedAfterFuncStateHasBeenPersisted(CreateFunctionStore());
    
    [TestMethod]
    public override Task PersistedCallbackIsInvokedAfterFuncWithScrapbookStateHasBeenPersisted()
        => PersistedCallbackIsInvokedAfterFuncWithScrapbookStateHasBeenPersisted(CreateFunctionStore());
    
    [TestMethod]
    public override Task PersistedCallbackIsInvokedAfterActionWithScrapbookStateHasBeenPersisted()
        => PersistedCallbackIsInvokedAfterActionWithScrapbookStateHasBeenPersisted(CreateFunctionStore());

    [TestMethod]
    public override Task PersistedCallbackIsInvokedAfterActionStateHasBeenPersisted()
        => PersistedCallbackIsInvokedAfterActionStateHasBeenPersisted(CreateFunctionStore());
    
    private IFunctionStore CreateFunctionStore([System.Runtime.CompilerServices.CallerMemberName] string callMemberName = "")
        => Sql.CreateAndInitializeStore(nameof(PersistedCallbackTests), callMemberName).Result;
}