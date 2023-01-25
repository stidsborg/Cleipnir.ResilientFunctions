using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Utils.Arbitrator;
using Cleipnir.ResilientFunctions.Utils.Monitor;
using Cleipnir.ResilientFunctions.Utils.Register;

namespace Cleipnir.ResilientFunctions.Testing;

public class TestHelper
{
    public FunctionId AnonymousFunctionId { get; } = new("Anonymous", "Anonymous");
    public InMemoryFunctionStore FunctionStore { get; }
    public Utilities Utilities { get; }
    
    public TestHelper() : this(new InMemoryFunctionStore()) { }

    public TestHelper(InMemoryFunctionStore functionStore)
    {
        FunctionStore = functionStore;
        Utilities = new Utilities(new InMemoryMonitor(), new InMemoryRegister(), new InMemoryArbitrator());
    } 

    public SpecificFunctionTestHelper For(FunctionId functionId) => new(functionId, FunctionStore, Utilities);
    public SpecificFunctionTestHelper ForAnonymous() => For(AnonymousFunctionId);

    public static TestHelper Create() => new TestHelper();
}