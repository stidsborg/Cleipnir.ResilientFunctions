using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
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

    public Task SetEffectResult(FlowId flowId, StoredEffect storedEffect)
        => _crashed
            ? Task.FromException(new TimeoutException())
            : _inner.SetEffectResult(flowId, storedEffect); 

    public Task<IReadOnlyList<StoredEffect>> GetEffectResults(FlowId flowId)
        => _crashed
            ? Task.FromException<IReadOnlyList<StoredEffect>>(new TimeoutException())
            : _inner.GetEffectResults(flowId);

    public Task DeleteEffectResult(FlowId flowId, EffectId effectId, bool isState)
        => _crashed
            ? Task.FromException(new TimeoutException())
            : _inner.DeleteEffectResult(flowId, effectId, isState);

    public Task Remove(FlowId flowId)
        => _crashed
            ? Task.FromException(new TimeoutException())
            : _inner.Remove(flowId);

    public bool Crashed
    {
        get => _crashed;
        set => _crashed = value;
    } 
}