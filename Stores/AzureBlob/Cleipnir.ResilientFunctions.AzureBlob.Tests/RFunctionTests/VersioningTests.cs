using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.AzureBlob.Tests.RFunctionTests;

[TestClass]
public class VersioningTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.VersioningTests
{
    [TestMethod]
    public override Task NonExistingParameterTypeResultsInFailedFunction()
        => NonExistingParameterTypeResultsInFailedFunction(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task WhenInputParameterOfRegisteredFunctionIsIncompatibleWithDeserializedTypeAnExceptionIsThrown()
        => WhenInputParameterOfRegisteredFunctionIsIncompatibleWithDeserializedTypeAnExceptionIsThrown(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task WhenScrapbookOfRegisteredFunctionIsIncompatibleWithDeserializedTypeAnExceptionIsThrown()
        => WhenScrapbookOfRegisteredFunctionIsIncompatibleWithDeserializedTypeAnExceptionIsThrown(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task RegisteredFunctionAcceptsTwoDifferentParameterTypesOfSameSubtype()
        => RegisteredFunctionAcceptsTwoDifferentParameterTypesOfSameSubtype(FunctionStoreFactory.FunctionStoreTask);
    
    [TestMethod]
    public override Task ReturnTypeCanBeParentTypeOfActualReturnedValue()
        => RegisteredFunctionAcceptsTwoDifferentParameterTypesOfSameSubtype(FunctionStoreFactory.FunctionStoreTask);
}