using Cleipnir.ResilientFunctions.Utils;
using Cleipnir.ResilientFunctions.Utils.Register;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public record Utilities(IRegister Register)
{
    public Utilities(IUnderlyingRegister underlyingRegister)
        : this(new Register(underlyingRegister)) {}
}