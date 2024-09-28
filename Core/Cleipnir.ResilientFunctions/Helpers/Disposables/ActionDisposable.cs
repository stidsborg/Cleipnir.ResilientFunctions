using System;

namespace Cleipnir.ResilientFunctions.Helpers.Disposables;

internal class ActionDisposable : IDisposable
{
    private readonly Action _dispose;
    private bool _disposed;
    private readonly object _sync = new();

    public ActionDisposable(Action dispose) => _dispose = dispose;

    public void Dispose()
    {
        lock (_sync)
        {
            if (_disposed) return;
            _disposed = true;
        }
        
        _dispose();
    }
}