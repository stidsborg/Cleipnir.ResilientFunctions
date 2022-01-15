using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class BarricadedTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.BarricadedTests
{
    [TestMethod]
    public override Task ABarricadedFunctionInvocationThrowsBarricadedException()
        => ABarricadedFunctionInvocationThrowsBarricadedException(new InMemoryFunctionStore());

    [TestMethod]
    public override Task AnExecutingFunctionCannotBeBarricaded()
        => AnExecutingFunctionCannotBeBarricaded(new InMemoryFunctionStore());
}