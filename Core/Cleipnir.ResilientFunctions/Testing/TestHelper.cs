using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Testing;

public class TestHelper
{
    public FunctionId AnonymousFunctionId { get; } = new("Anonymous", "Anonymous");
    public InMemoryFunctionStore FunctionStore { get; }
    
    public TestHelper() : this(new InMemoryFunctionStore()) { }
    public TestHelper(InMemoryFunctionStore functionStore) => FunctionStore = functionStore;

    public SpecificFunctionTestHelper For(FunctionId functionId) => new(functionId, FunctionStore);
    public SpecificFunctionTestHelper ForAnonymous() => For(AnonymousFunctionId);

    public static TestHelper Create() => new TestHelper();
}