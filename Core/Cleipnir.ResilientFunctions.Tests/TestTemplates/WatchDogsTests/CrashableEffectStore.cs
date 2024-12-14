using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests;

public class CrashableEffectStore : IEffectsStore
{
    private readonly IEffectsStore _inner;
    private volatile bool _crashed;

    public CrashableEffectStore(IEffectsStore inner)
    {
        _inner = inner;
    }

    public Task Initialize()
        => _crashed
            ? Task.FromException(new TimeoutException())
            : _inner.Initialize();

    public Task Truncate()
        => _crashed
            ? Task.FromException(new TimeoutException())
            : _inner.Truncate();

    public Task SetEffectResult(StoredId storedId, StoredEffect storedEffect)
        => _crashed
            ? Task.FromException(new TimeoutException())
            : _inner.SetEffectResult(storedId, storedEffect);

    public Task SetEffectResults(StoredId storedId, IReadOnlyList<StoredEffect> storedEffects)
        => _crashed
            ? Task.FromException(new TimeoutException())
            : _inner.SetEffectResults(storedId, storedEffects);

    public Task<IReadOnlyList<StoredEffect>> GetEffectResults(StoredId storedId)
        => _crashed
            ? Task.FromException<IReadOnlyList<StoredEffect>>(new TimeoutException())
            : _inner.GetEffectResults(storedId);

    public Task DeleteEffectResult(StoredId storedId, StoredEffectId effectId, bool isState)
        => _crashed
            ? Task.FromException(new TimeoutException())
            : _inner.DeleteEffectResult(storedId, effectId, isState);

    public Task Remove(StoredId storedId)
        => _crashed
            ? Task.FromException(new TimeoutException())
            : _inner.Remove(storedId);

    public bool Crashed
    {
        get => _crashed;
        set => _crashed = value;
    } 
}