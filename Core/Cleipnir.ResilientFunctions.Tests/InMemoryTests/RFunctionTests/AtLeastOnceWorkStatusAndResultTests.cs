using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class AtLeastOnceWorkStatusAndResultTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.AtLeastOnceWorkStatusAndResultTests
{
    [TestMethod]
    public override Task AtLeastOnceWorkIsExecutedMultipleTimesWhenNotCompleted()
        => AtLeastOnceWorkIsExecutedMultipleTimesWhenNotCompleted(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task AtLeastOnceWorkWithCallIdIsExecutedMultipleTimesWhenNotCompleted()
        => AtLeastOnceWorkWithCallIdIsExecutedMultipleTimesWhenNotCompleted(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task AtLeastOnceWorkWithCallIdAndGenericResultIsExecutedMultipleTimesWhenNotCompleted()
        => AtLeastOnceWorkWithCallIdAndGenericResultIsExecutedMultipleTimesWhenNotCompleted(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task CompletedAtLeastOnceWorkIsNotExecutedMultipleTimes()
        => CompletedAtLeastOnceWorkIsNotExecutedMultipleTimes(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task CompletedAtLeastOnceWorkWithCallIdIsNotExecutedMultipleTimes()
        => CompletedAtLeastOnceWorkWithCallIdIsNotExecutedMultipleTimes(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ReferencingGetOnlyPropertyThrowsException()
        => ReferencingGetOnlyPropertyThrowsException(Utils.CreateInMemoryFunctionStoreTask());
}