using System;
using System.Collections.Generic;

namespace Cleipnir.ResilientFunctions.Helpers.Disposables;

public static class Disposable
{
    public static IDisposable Combine(params IDisposable[] disposables) => new CombinedDisposables(disposables);
    public static IDisposable Combine(IEnumerable<IDisposable> disposables) => new CombinedDisposables(disposables);
    public static IDisposable NoOp() => NoOpDisposable.Instance;
}