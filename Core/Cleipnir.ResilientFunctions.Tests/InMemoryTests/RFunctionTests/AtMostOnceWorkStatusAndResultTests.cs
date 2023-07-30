using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class AtMostOnceWorkStatusAndResultTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.AtMostOnceWorkStatusAndResultTests
{
    [TestMethod]
    public override Task AtMostOnceWorkIsNotExecutedMultipleTimes()
        => AtMostOnceWorkIsNotExecutedMultipleTimes(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task AtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes()
        => AtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task AtMostOnceWorkWithCallIdIsNotExecutedMultipleTimesUsingEventSource()
        => AtMostOnceWorkWithCallIdIsNotExecutedMultipleTimesUsingEventSource(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task CompletedAtMostOnceWorkIsNotExecutedMultipleTimes()
        => CompletedAtMostOnceWorkIsNotExecutedMultipleTimes(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task CompletedAtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes()
        => CompletedAtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task CompletedAtMostOnceWorkWithCallIdIsNotExecutedMultipleTimesUsingEventSource()
        => CompletedAtMostOnceWorkWithCallIdIsNotExecutedMultipleTimesUsingEventSource(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ReferencingGetOnlyPropertyThrowsException()
        => ReferencingGetOnlyPropertyThrowsException(Utils.CreateInMemoryFunctionStoreTask());
}