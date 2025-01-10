using System;
using System.Collections.Generic;
using System.Threading;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

public class CombinableDisposable : IDisposable
{
    private readonly List<IDisposable> _disposables = new();
    private bool _disposed;
    private readonly Lock _sync = new();

    public void Add(IDisposable disposable)
    {
        lock (_sync)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(CombinableDisposable));
            
            _disposables.Add(disposable);
        }
    }
    
    public void Dispose()
    {
        lock (_sync)
        {
            if (_disposed) return;
            _disposed = true;
            
            foreach (var disposable in _disposables)
                disposable.Dispose();
            
            _disposables.Clear();
        }
    }
}