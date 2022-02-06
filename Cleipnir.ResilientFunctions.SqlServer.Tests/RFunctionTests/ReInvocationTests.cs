using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.RFunctionTests;

[TestClass]
public class ReInvocationTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.ReInvocationTests
{
    [TestMethod]
    public override Task FailedRActionCanBeReInvoked()
        => FailedRActionCanBeReInvoked(Sql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task FailedRActionWithScrapbookCanBeReInvoked()
        => FailedRActionWithScrapbookCanBeReInvoked(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task FailedRFuncCanBeReInvoked()
        => FailedRFuncCanBeReInvoked(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task FailedRFuncWithScrapbookCanBeReInvoked()
        => FailedRFuncWithScrapbookCanBeReInvoked(Sql.AutoCreateAndInitializeStore());
}