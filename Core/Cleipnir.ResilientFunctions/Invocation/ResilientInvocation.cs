using System.Threading;

namespace Cleipnir.ResilientFunctions.Invocation;

public static class ResilientInvocation
{
    private static readonly AsyncLocal<InvocationMode> _mode = new();
    public static InvocationMode Mode 
    { 
        get => _mode.Value;
        internal set => _mode.Value = value;
    }
}

public enum InvocationMode
{
    Direct = 0,
    Retry = 1 
}