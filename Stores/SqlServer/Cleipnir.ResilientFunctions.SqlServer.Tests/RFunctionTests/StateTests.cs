using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.RFunctionTests;

[TestClass]
public class StateTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.StateTests
{
    [TestMethod]
    public override Task StateCanBeFetchedFromFuncRegistration()
        => StateCanBeFetchedFromFuncRegistration(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ExistingStateCanBeDeleted()
        => ExistingStateCanBeDeleted(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task StateCanBeFetchedFromActionRegistration()
        => StateCanBeFetchedFromActionRegistration(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ExistingDefaultStateCanBeDeleted()
        => ExistingDefaultStateCanBeDeleted(FunctionStoreFactory.Create());
}