using System;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

public class DisposableEmpty : IDisposable
{
    public static DisposableEmpty Instance { get; } = new DisposableEmpty();
    
    public void Dispose() { }
}