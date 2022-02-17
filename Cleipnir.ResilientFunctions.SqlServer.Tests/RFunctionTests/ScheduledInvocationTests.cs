using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.RFunctionTests;

[TestClass]
public class ScheduledInvocationTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.ScheduledInvocationTests
{
    [TestMethod]
    public override Task ScheduledFunctionIsInvokedAfterFuncStateHasBeenPersisted()
        => ScheduledFunctionIsInvokedAfterFuncStateHasBeenPersisted(Sql.AutoCreateAndInitializeStore().Result);
    
    [TestMethod]
    public override Task ScheduledFunctionIsInvokedAfterFuncWithScrapbookStateHasBeenPersisted()
        => ScheduledFunctionIsInvokedAfterFuncWithScrapbookStateHasBeenPersisted(Sql.AutoCreateAndInitializeStore().Result);
    
    [TestMethod]
    public override Task ScheduledFunctionIsInvokedAfterActionWithScrapbookStateHasBeenPersisted()
        => ScheduledFunctionIsInvokedAfterActionWithScrapbookStateHasBeenPersisted(Sql.AutoCreateAndInitializeStore().Result);

    [TestMethod]
    public override Task ScheduledFunctionIsInvokedAfterActionStateHasBeenPersisted()
        => ScheduledFunctionIsInvokedAfterActionStateHasBeenPersisted(Sql.AutoCreateAndInitializeStore().Result);
}