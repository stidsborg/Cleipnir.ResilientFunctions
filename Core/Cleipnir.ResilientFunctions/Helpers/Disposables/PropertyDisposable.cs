using System;

namespace Cleipnir.ResilientFunctions.Helpers.Disposables;

internal class PropertyDisposable : IDisposable
{
    private volatile bool _disposed = false;

    public bool Disposed => _disposed;
    
    public void Dispose() => _disposed = true;
}