using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.RFunctionTests;

[TestClass]
public class BarricadedTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.BarricadedTests
{
    [TestMethod]
    public override Task ABarricadedFunctionInvocationThrowsBarricadedException()
        => ABarricadedFunctionInvocationThrowsBarricadedException(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task AnExecutingFunctionCannotBeBarricaded()
        => AnExecutingFunctionCannotBeBarricaded(Sql.AutoCreateAndInitializeStore());
}