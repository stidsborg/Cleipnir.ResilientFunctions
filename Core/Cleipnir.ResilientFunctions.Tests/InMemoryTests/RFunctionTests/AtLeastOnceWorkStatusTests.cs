using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class AtLeastOnceWorkStatusTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.AtLeastOnceWorkStatusTests
{
    [TestMethod]
    public override Task AtLeastOnceWorkIsExecutedMultipleTimesWhenNotCompleted()
        => AtLeastOnceWorkIsExecutedMultipleTimesWhenNotCompleted(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task AtLeastOnceWorkWithCallIdIsExecutedMultipleTimesWhenNotCompleted()
        => AtLeastOnceWorkWithCallIdIsExecutedMultipleTimesWhenNotCompleted(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task AtLeastOnceWorkWithCallIdIsExecutedMultipleTimesWhenNotCompletedUsingEventSource()
        => AtLeastOnceWorkWithCallIdIsExecutedMultipleTimesWhenNotCompletedUsingEventSource(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task CompletedAtLeastOnceWorkIsNotExecutedMultipleTimes()
        => CompletedAtLeastOnceWorkIsNotExecutedMultipleTimes(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task CompletedAtLeastOnceWorkWithCallIdIsNotExecutedMultipleTimes()
        => CompletedAtLeastOnceWorkWithCallIdIsNotExecutedMultipleTimes(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task CompletedAtLeastOnceWorkWithCallIdIsNotExecutedMultipleTimesUsingEventSource()
        => CompletedAtLeastOnceWorkWithCallIdIsNotExecutedMultipleTimesUsingEventSource(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ReferencingGetOnlyPropertyThrowsException()
        => ReferencingGetOnlyPropertyThrowsException(Utils.CreateInMemoryFunctionStoreTask());
}