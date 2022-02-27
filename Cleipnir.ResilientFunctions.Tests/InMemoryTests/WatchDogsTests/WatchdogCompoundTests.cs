using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.WatchDogsTests;

[TestClass]
public class WatchdogCompoundTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests.WatchdogCompoundTests
{
    [TestMethod]
    public override Task FunctionCompoundTest() 
        => FunctionCompoundTest(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());
    
    [TestMethod]
    public override Task FunctionWithScrapbookCompoundTest() 
        => FunctionWithScrapbookCompoundTest(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());
    
    [TestMethod]
    public override Task ActionCompoundTest()
        => ActionCompoundTest(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());
    
    [TestMethod]
    public override Task ActionWithScrapbookCompoundTest()
        => ActionWithScrapbookCompoundTest(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());
}