using System;

namespace Cleipnir.ResilientFunctions.Helpers.Disposables;

public class ActionDisposable : IDisposable
{
    private readonly System.Action _dispose;
    private bool _disposed;
    private readonly object _sync = new();

    public ActionDisposable(System.Action dispose) => _dispose = dispose;

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