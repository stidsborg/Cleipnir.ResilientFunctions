using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MySQL.Tests.RFunctionTests;

[TestClass]
public class VersioningTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.VersioningTests
{
    [TestMethod]
    public override Task NonExistingParameterTypeResultsInFailedFunction()
        => NonExistingParameterTypeResultsInFailedFunction(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task WhenInputParameterOfRegisteredFunctionIsIncompatibleWithDeserializedTypeAnExceptionIsThrown()
        => WhenInputParameterOfRegisteredFunctionIsIncompatibleWithDeserializedTypeAnExceptionIsThrown(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task WhenScrapbookOfRegisteredFunctionIsIncompatibleWithDeserializedTypeAnExceptionIsThrown()
        => WhenScrapbookOfRegisteredFunctionIsIncompatibleWithDeserializedTypeAnExceptionIsThrown(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task RegisteredFunctionAcceptsTwoDifferentParameterTypesOfSameSubtype()
        => RegisteredFunctionAcceptsTwoDifferentParameterTypesOfSameSubtype(Sql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task ReturnTypeCanBeParentTypeOfActualReturnedValue()
        => RegisteredFunctionAcceptsTwoDifferentParameterTypesOfSameSubtype(Sql.AutoCreateAndInitializeStore());
}