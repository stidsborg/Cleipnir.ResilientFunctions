using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Tests.RFunctionTests;

[TestClass]
public class AtMostOnceWorkStatusAndResultTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.AtMostOnceWorkStatusAndResultTests
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
    public override Task ReferencingGetOnlyPropertyThrowsException()
        => ReferencingGetOnlyPropertyThrowsException(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task AtMostOnceWorkWithCallIdIsNotExecutedMultipleTimesUsingEventSource()
        => AtMostOnceWorkWithCallIdIsNotExecutedMultipleTimesUsingEventSource(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task CompletedAtMostOnceWorkWithCallIdIsNotExecutedMultipleTimesUsingEventSource()
        => CompletedAtMostOnceWorkWithCallIdIsNotExecutedMultipleTimesUsingEventSource(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task AtMostOnceWorkWithCallIdAndGenericResultIsNotExecutedMultipleTimes()
        => AtMostOnceWorkWithCallIdAndGenericResultIsNotExecutedMultipleTimes(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task CompletedAtMostOnceWorkWithCallIdAndGenericResultIsNotExecutedMultipleTimes()
        => CompletedAtMostOnceWorkWithCallIdAndGenericResultIsNotExecutedMultipleTimes(FunctionStoreFactory.Create());
}