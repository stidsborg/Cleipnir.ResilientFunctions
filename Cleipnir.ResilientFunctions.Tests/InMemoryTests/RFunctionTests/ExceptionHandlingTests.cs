using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class ExceptionHandlingTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.ExceptionHandlingTests
{
    [TestMethod]
    public override Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnFunc()
        => UnhandledExceptionIsRethrownWhenEnsuringSuccessOnFunc(
            new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask()
        );

    [TestMethod]
    public override Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnFuncWithScrapbook()
        => UnhandledExceptionIsRethrownWhenEnsuringSuccessOnFuncWithScrapbook(
            new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask()
        );
    
    [TestMethod]
    public override Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnAction()
        => UnhandledExceptionIsRethrownWhenEnsuringSuccessOnAction(
            new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask()
        );

    [TestMethod]
    public override Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnActionWithScrapbook()
        => UnhandledExceptionIsRethrownWhenEnsuringSuccessOnActionWithScrapbook(
            new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask()
        );
}