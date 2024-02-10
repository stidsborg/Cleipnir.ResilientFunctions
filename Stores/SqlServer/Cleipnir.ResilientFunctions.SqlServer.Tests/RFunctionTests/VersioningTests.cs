using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.RFunctionTests;

[TestClass]
public class VersioningTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.VersioningTests
{
    [TestMethod]
    public override Task NonExistingParameterTypeResultsInFailedFunction()
        => NonExistingParameterTypeResultsInFailedFunction(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task WhenInputParameterOfRegisteredFunctionIsIncompatibleWithDeserializedTypeAnExceptionIsThrown()
        => WhenInputParameterOfRegisteredFunctionIsIncompatibleWithDeserializedTypeAnExceptionIsThrown(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task WhenStateOfRegisteredFunctionIsIncompatibleWithDeserializedTypeAnExceptionIsThrown()
        => WhenStateOfRegisteredFunctionIsIncompatibleWithDeserializedTypeAnExceptionIsThrown(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task RegisteredFunctionAcceptsTwoDifferentParameterTypesOfSameSubtype()
        => RegisteredFunctionAcceptsTwoDifferentParameterTypesOfSameSubtype(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ReturnTypeCanBeParentTypeOfActualReturnedValue()
        => RegisteredFunctionAcceptsTwoDifferentParameterTypesOfSameSubtype(FunctionStoreFactory.Create());
}