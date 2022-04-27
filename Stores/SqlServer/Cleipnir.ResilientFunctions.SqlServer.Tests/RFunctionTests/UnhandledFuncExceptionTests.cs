using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.RFunctionTests;

[TestClass]
public class UnhandledFuncExceptionTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.UnhandledFuncExceptionTests
{
    [TestMethod]
    public override Task UnhandledExceptionResultsInPostponedFunc()
        => UnhandledExceptionResultsInPostponedFunc(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task UnhandledExceptionResultsInPostponedFuncWithScrapbook()
        => UnhandledExceptionResultsInPostponedFuncWithScrapbook(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task UnhandledExceptionResultsInPostponedAction()
        => UnhandledExceptionResultsInPostponedAction(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task UnhandledExceptionResultsInPostponedActionWithScrapbook()
        => UnhandledExceptionResultsInPostponedActionWithScrapbook(Sql.AutoCreateAndInitializeStore());
}