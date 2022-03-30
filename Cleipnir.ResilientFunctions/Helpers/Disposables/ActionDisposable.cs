using System;

namespace Cleipnir.ResilientFunctions.Helpers.Disposables;

public class ActionDisposable : IDisposable
{
    private readonly Action _dispose;
    private bool _disposed;

    public ActionDisposable(Action dispose) => _dispose = dispose;

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _dispose();
    }
}