using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.RFunctionTests;

[TestClass]
public class AtMostOnceWorkStatusAndResultTests : ResilientFunctions.Tests.TestTemplates.FunctionTests.AtMostOnceWorkStatusAndResultTests
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

    [TestMethod]
    public override Task CompletedAtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes()
        => CompletedAtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task AtMostOnceWorkWithCallIdAndGenericResultIsNotExecutedMultipleTimes()
        => AtMostOnceWorkWithCallIdAndGenericResultIsNotExecutedMultipleTimes(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task CompletedAtMostOnceWorkWithCallIdAndGenericResultIsNotExecutedMultipleTimes()
        => CompletedAtMostOnceWorkWithCallIdAndGenericResultIsNotExecutedMultipleTimes(FunctionStoreFactory.Create());
}