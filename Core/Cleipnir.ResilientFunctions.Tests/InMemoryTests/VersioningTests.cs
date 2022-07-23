using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class VersioningTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.VersioningTests
{
    [TestMethod]
    public override Task NonExistingParameterTypeResultsInFailedFunction()
        => NonExistingParameterTypeResultsInFailedFunction(CreateInMemoryFunctionStore());

    [TestMethod]
    public override Task WhenInputParameterOfRegisteredFunctionIsIncompatibleWithDeserializedTypeAnExceptionIsThrown()
        => WhenInputParameterOfRegisteredFunctionIsIncompatibleWithDeserializedTypeAnExceptionIsThrown(CreateInMemoryFunctionStore());

    [TestMethod]
    public override Task RegisteredFunctionAcceptsTwoDifferentParameterTypesOfSameSubtype()
        => RegisteredFunctionAcceptsTwoDifferentParameterTypesOfSameSubtype(CreateInMemoryFunctionStore());

    private static Task<IFunctionStore> CreateInMemoryFunctionStore()
        => new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask();
}