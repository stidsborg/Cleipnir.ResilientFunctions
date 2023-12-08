using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Tests.RFunctionTests;

[TestClass]
public class AtLeastOnceWorkStatusAndResultTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.AtLeastOnceWorkStatusAndResultTests
{
    [TestMethod]
    public override Task AtLeastOnceWorkIsExecutedMultipleTimesWhenNotCompleted()
        => AtLeastOnceWorkIsExecutedMultipleTimesWhenNotCompleted(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task AtLeastOnceWorkWithCallIdIsExecutedMultipleTimesWhenNotCompleted()
        => AtLeastOnceWorkWithCallIdIsExecutedMultipleTimesWhenNotCompleted(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task AtLeastOnceWorkWithCallIdAndGenericResultIsExecutedMultipleTimesWhenNotCompleted()
        => AtLeastOnceWorkWithCallIdAndGenericResultIsExecutedMultipleTimesWhenNotCompleted(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task CompletedAtLeastOnceWorkIsNotExecutedMultipleTimes()
        => CompletedAtLeastOnceWorkIsNotExecutedMultipleTimes(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task CompletedAtLeastOnceWorkWithCallIdIsNotExecutedMultipleTimes()
        => CompletedAtLeastOnceWorkWithCallIdIsNotExecutedMultipleTimes(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ReferencingGetOnlyPropertyThrowsException()
        => ReferencingGetOnlyPropertyThrowsException(FunctionStoreFactory.Create());
}