using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class ReInvocationTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.ReInvocationTests
{
    [TestMethod]
    public override Task FailedRActionCanBeReInvoked()
        => FailedRActionCanBeReInvoked(new InMemoryFunctionStore());
    
    [TestMethod]
    public override Task FailedRActionWithScrapbookCanBeReInvoked()
        => FailedRActionWithScrapbookCanBeReInvoked(new InMemoryFunctionStore());

    [TestMethod]
    public override Task FailedRFuncCanBeReInvoked()
        => FailedRFuncCanBeReInvoked(new InMemoryFunctionStore());

    [TestMethod]
    public override Task FailedRFuncWithScrapbookCanBeReInvoked()
        => FailedRFuncWithScrapbookCanBeReInvoked(new InMemoryFunctionStore());
}