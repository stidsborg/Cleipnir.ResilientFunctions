using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MariaDb.Tests.RFunctionTests;

[TestClass]
public class StateTests : ResilientFunctions.Tests.TestTemplates.FunctionTests.StateTests
{
    [TestMethod]
    public override Task StateCanBeFetchedFromFuncRegistration()
        => StateCanBeFetchedFromFuncRegistration(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ExistingDefaultStateCanBeDeleted()
        => ExistingDefaultStateCanBeDeleted(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExistingStateCanBeDeleted()
        => ExistingStateCanBeDeleted(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task StateCanBeFetchedFromActionRegistration()
        => StateCanBeFetchedFromActionRegistration(FunctionStoreFactory.Create());
}