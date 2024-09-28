using System;

namespace Cleipnir.ResilientFunctions.Helpers.Disposables;

internal class NoOpDisposable : IDisposable
{
    public static IDisposable Instance { get; } = new NoOpDisposable();
    public void Dispose() { }
}