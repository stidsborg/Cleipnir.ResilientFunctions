using System;
using System.Collections.Generic;

namespace Cleipnir.ResilientFunctions.Helpers.Disposables;

public class CombinedDisposables : IDisposable
{
    private readonly IEnumerable<IDisposable> _disposables;

    public CombinedDisposables(IDisposable[] disposables) => _disposables = disposables;
    public CombinedDisposables(IEnumerable<IDisposable> disposables) => _disposables = disposables;

    public void Dispose()
    {
        foreach (var disposable in _disposables)
            disposable.Dispose();
    }
}