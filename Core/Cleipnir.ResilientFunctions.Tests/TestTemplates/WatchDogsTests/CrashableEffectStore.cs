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

    public Task SetEffectResults(StoredId storedId, IReadOnlyList<StoredEffect> upsertEffects, IReadOnlyList<StoredEffectId> removeEffects)
        => _crashed
            ? Task.FromException(new TimeoutException())
            : _inner.SetEffectResults(storedId, upsertEffects, removeEffects);

    public Task<IReadOnlyList<StoredEffect>> GetEffectResults(StoredId storedId)
        => _crashed
            ? Task.FromException<IReadOnlyList<StoredEffect>>(new TimeoutException())
            : _inner.GetEffectResults(storedId);

    public Task DeleteEffectResult(StoredId storedId, StoredEffectId effectId)
        => _crashed
            ? Task.FromException(new TimeoutException())
            : _inner.DeleteEffectResult(storedId, effectId);

    public Task DeleteEffectResults(StoredId storedId, IReadOnlyList<StoredEffectId> effectIds)
        => _crashed
            ? Task.FromException(new TimeoutException())
            : _inner.DeleteEffectResults(storedId, effectIds);

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