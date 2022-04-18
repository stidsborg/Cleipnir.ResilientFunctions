using System;

namespace Cleipnir.ResilientFunctions.Helpers.Disposables;

public static class Disposable
{
    public static IDisposable Combine(params IDisposable[] disposables) => new CombinedDisposables(disposables);
}