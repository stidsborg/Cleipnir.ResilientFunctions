using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.AzureBlob.Tests.RFunctionTests;

[TestClass]
public class AtMostOnceWorkStatusTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.AtMostOnceWorkStatusTests
{
    [TestMethod]
    public override Task AtMostOnceWorkIsNotExecutedMultipleTimes()
        => AtMostOnceWorkIsNotExecutedMultipleTimes(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task AtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes()
        => AtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task CompletedAtMostOnceWorkIsNotExecutedMultipleTimes()
        => CompletedAtMostOnceWorkIsNotExecutedMultipleTimes(FunctionStoreFactory.Create());
}