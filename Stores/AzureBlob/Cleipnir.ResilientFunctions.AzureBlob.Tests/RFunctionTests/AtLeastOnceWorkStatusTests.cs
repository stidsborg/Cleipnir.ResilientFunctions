using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.AzureBlob.Tests.RFunctionTests;

[TestClass]
public class AtLeastOnceWorkStatusTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.AtLeastOnceWorkStatusTests
{
    [TestMethod]
    public override Task AtLeastOnceWorkIsExecutedMultipleTimesWhenNotCompleted()
        => AtLeastOnceWorkIsExecutedMultipleTimesWhenNotCompleted(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task AtLeastOnceWorkWithCallIdIsExecutedMultipleTimesWhenNotCompleted()
        => AtLeastOnceWorkWithCallIdIsExecutedMultipleTimesWhenNotCompleted(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task CompletedAtLeastOnceWorkIsNotExecutedMultipleTimes()
        => CompletedAtLeastOnceWorkIsNotExecutedMultipleTimes(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task CompletedAtLeastOnceWorkWithCallIdIsNotExecutedMultipleTimes()
        => CompletedAtLeastOnceWorkWithCallIdIsNotExecutedMultipleTimes(FunctionStoreFactory.Create());
}