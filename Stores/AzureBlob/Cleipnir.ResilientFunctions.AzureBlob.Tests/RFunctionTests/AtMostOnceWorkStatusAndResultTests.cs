using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.AzureBlob.Tests.RFunctionTests;

[TestClass]
public class AtMostOnceWorkStatusAndResultTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.AtMostOnceWorkStatusAndResultTests
{
    [TestMethod]
    public override Task AtMostOnceWorkIsNotExecutedMultipleTimes()
        => AtMostOnceWorkIsNotExecutedMultipleTimes(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task AtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes()
        => AtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task AtMostOnceWorkWithCallIdIsNotExecutedMultipleTimesUsingEventSource()
        => AtMostOnceWorkWithCallIdIsNotExecutedMultipleTimesUsingEventSource(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task CompletedAtMostOnceWorkIsNotExecutedMultipleTimes()
        => CompletedAtMostOnceWorkIsNotExecutedMultipleTimes(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task CompletedAtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes()
        => CompletedAtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task CompletedAtMostOnceWorkWithCallIdIsNotExecutedMultipleTimesUsingEventSource()
        => CompletedAtMostOnceWorkWithCallIdIsNotExecutedMultipleTimesUsingEventSource(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task ReferencingGetOnlyPropertyThrowsException()
        => ReferencingGetOnlyPropertyThrowsException(FunctionStoreFactory.FunctionStoreTask);
    
    [TestMethod]
    public override Task AtMostOnceWorkWithCallIdAndGenericResultIsNotExecutedMultipleTimes()
        => AtMostOnceWorkWithCallIdAndGenericResultIsNotExecutedMultipleTimes(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task CompletedAtMostOnceWorkWithCallIdAndGenericResultIsNotExecutedMultipleTimes()
        => CompletedAtMostOnceWorkWithCallIdAndGenericResultIsNotExecutedMultipleTimes(FunctionStoreFactory.FunctionStoreTask);
}