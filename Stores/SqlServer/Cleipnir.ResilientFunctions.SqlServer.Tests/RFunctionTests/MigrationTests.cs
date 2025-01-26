using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.RFunctionTests;

[TestClass]
public class MigrationTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.MigrationTests
{
    [TestMethod]
    public override Task MigrationExceptionIsThrownOnVersionMismatch()
        => MigrationExceptionIsThrownOnVersionMismatch(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task InitializeSucceedsOnVersionMatch()
        => InitializeSucceedsOnVersionMatch(FunctionStoreFactory.Create());
}