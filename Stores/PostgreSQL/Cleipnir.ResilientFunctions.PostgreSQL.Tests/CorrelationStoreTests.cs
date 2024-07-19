using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Tests;

[TestClass]
public class CorrelationStoreTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.CorrelationStoreTests
{
    [TestMethod]
    public override Task SunshineScenario()
        => SunshineScenario(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task TwoDifferentFunctionsCanUseTheSameCorrelationId()
        => TwoDifferentFunctionsCanUseTheSameCorrelationId(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FunctionCorrelationsCanBeDeleted()
        => FunctionCorrelationsCanBeDeleted(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SingleFunctionCorrelationCanBeDeleted()
        => SingleFunctionCorrelationCanBeDeleted(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task SingleFunctionCanHaveMultipleCorrelations()
        => SingleFunctionCanHaveMultipleCorrelations(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task FunctionInstancesCanBeFetchedForFunctionTypeAndCorrelation()
        => FunctionInstancesCanBeFetchedForFunctionTypeAndCorrelation(FunctionStoreFactory.Create());
}