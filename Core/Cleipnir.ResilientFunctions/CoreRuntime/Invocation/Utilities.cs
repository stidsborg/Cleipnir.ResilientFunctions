using Cleipnir.ResilientFunctions.Utils;
using Cleipnir.ResilientFunctions.Utils.Arbitrator;
using Cleipnir.ResilientFunctions.Utils.Register;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public record Utilities(IRegister Register, IArbitrator Arbitrator)
{
    public Utilities(IUnderlyingRegister underlyingRegister) 
        : this(
            new Register(underlyingRegister),
            new Arbitrator(underlyingRegister)
        ) {}
}