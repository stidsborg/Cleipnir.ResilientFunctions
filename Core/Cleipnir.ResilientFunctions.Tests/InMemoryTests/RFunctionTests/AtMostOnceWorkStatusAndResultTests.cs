using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class AtMostOnceWorkStatusAndResultTests : TestTemplates.FunctionTests.AtMostOnceWorkStatusAndResultTests
{
    [TestMethod]
    public override Task AtMostOnceWorkIsNotExecutedMultipleTimes()
        => AtMostOnceWorkIsNotExecutedMultipleTimes(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task AtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes()
        => AtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task AtMostOnceWorkWithCallIdAndGenericResultIsNotExecutedMultipleTimes()
        => AtMostOnceWorkWithCallIdAndGenericResultIsNotExecutedMultipleTimes(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task CompletedAtMostOnceWorkIsNotExecutedMultipleTimes()
        => CompletedAtMostOnceWorkIsNotExecutedMultipleTimes(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task CompletedAtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes()
        => CompletedAtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task CompletedAtMostOnceWorkWithCallIdAndGenericResultIsNotExecutedMultipleTimes()
        => CompletedAtMostOnceWorkWithCallIdAndGenericResultIsNotExecutedMultipleTimes(Utils.CreateInMemoryFunctionStoreTask());
}