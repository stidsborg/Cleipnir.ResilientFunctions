using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MongoDB.Tests.RFunctionTests;

[TestClass]
public class VersioningTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.VersioningTests
{
    [TestMethod]
    public override Task NonExistingParameterTypeResultsInFailedFunction()
        => NonExistingParameterTypeResultsInFailedFunction(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task WhenInputParameterOfRegisteredFunctionIsIncompatibleWithDeserializedTypeAnExceptionIsThrown()
        => WhenInputParameterOfRegisteredFunctionIsIncompatibleWithDeserializedTypeAnExceptionIsThrown(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task WhenScrapbookOfRegisteredFunctionIsIncompatibleWithDeserializedTypeAnExceptionIsThrown()
        => WhenScrapbookOfRegisteredFunctionIsIncompatibleWithDeserializedTypeAnExceptionIsThrown(NoSql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task RegisteredFunctionAcceptsTwoDifferentParameterTypesOfSameSubtype()
        => RegisteredFunctionAcceptsTwoDifferentParameterTypesOfSameSubtype(NoSql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task ReturnTypeCanBeParentTypeOfActualReturnedValue()
        => RegisteredFunctionAcceptsTwoDifferentParameterTypesOfSameSubtype(NoSql.AutoCreateAndInitializeStore());
}