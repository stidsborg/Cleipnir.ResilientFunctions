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

    public Task SetEffectResult(StoredId storedId, StoredEffect storedEffect, IStorageSession? session)
        => _crashed
            ? Task.FromException(new TimeoutException())
            : _inner.SetEffectResult(storedId, storedEffect, session);

    public Task SetEffectResults(StoredId storedId, IReadOnlyList<StoredEffectChange> changes, IStorageSession? session)
        => _crashed
            ? Task.FromException(new TimeoutException())
            : _inner.SetEffectResults(storedId, changes, session);

    public Task<IReadOnlyList<StoredEffect>> GetEffectResults(StoredId storedId)
        => _crashed
            ? Task.FromException<IReadOnlyList<StoredEffect>>(new TimeoutException())
            : _inner.GetEffectResults(storedId);

    public Task<Dictionary<StoredId, List<StoredEffect>>> GetEffectResults(IEnumerable<StoredId> storedIds)
        => _crashed
            ? Task.FromException<Dictionary<StoredId, List<StoredEffect>>>(new TimeoutException())
            : _inner.GetEffectResults(storedIds);

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