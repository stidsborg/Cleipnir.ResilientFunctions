using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal class ReplicaWatchdog(ClusterInfo clusterInfo, IFunctionStore functionStore, TimeSpan checkFrequency, UnhandledExceptionHandler unhandledExceptionHandler) : IDisposable
{
    private volatile bool _disposed;
    private bool _started;
    private bool _initialized;
    private readonly Dictionary<StoredReplica, int> _strikes = new();
    private IReplicaStore ReplicaStore => functionStore.ReplicaStore;
    
    public async Task Start()
    {
        var originalValue = Interlocked.CompareExchange(ref _started, value: true, comparand: false);
        if (originalValue)
            return;
        
        if (!_initialized)
            await Initialize();
        
        _ = Task.Run(Run);
    }

    public async Task Initialize()
    {
        await ReplicaStore.Insert(clusterInfo.ReplicaId);
        var replicas = await ReplicaStore.GetAll();
        var offset = CalculateOffset(replicas.Select(sr => sr.ReplicaId), clusterInfo.ReplicaId);
        if (offset is null)
            throw new InvalidOperationException("Replica offset was null after initialization");

        var ownerReplicas = await functionStore.GetOwnerReplicas();
        
        //handle crashed orphan functions
        var crashedReplicas = ownerReplicas
            .Where(ownerReplicaId => replicas.All(storedReplica => storedReplica.ReplicaId != ownerReplicaId))
            .ToList();
        foreach (var crashedReplicaId in crashedReplicas)
            await functionStore.RescheduleCrashedFunctions(crashedReplicaId);
        
        clusterInfo.ReplicaCount = replicas.Count;
        clusterInfo.Offset = offset.Value;
        _initialized = true;
    }

    private async Task Run()
    {
        while (!_disposed)
        {
            try
            {
                await PerformIteration();
            }
            catch (Exception ex)
            {
                unhandledExceptionHandler.Invoke(new FrameworkException("ReplicaWatchdog failed during iteration", ex));
            }
            
            await Task.Delay(checkFrequency);
        }
    }

   public async Task PerformIteration()
    {
        await ReplicaStore.UpdateHeartbeat(clusterInfo.ReplicaId);
        
        var storedReplicas = await ReplicaStore.GetAll();
        var offset = CalculateOffset(storedReplicas.Select(sr => sr.ReplicaId), clusterInfo.ReplicaId);

        if (offset is not null)
        {
            clusterInfo.Offset = offset.Value;
            clusterInfo.ReplicaCount = storedReplicas.Count;
        }
        else
        {
            await ReplicaStore.Insert(clusterInfo.ReplicaId);
            _strikes.Clear();
            await PerformIteration();
        }
        
        IncrementStrikesCount();
        ClearNonRelevantStrikes(storedReplicas);
        AddNewStrikes(storedReplicas);
        
        await DeleteStrikedOutReplicas();
    }

    public static int? CalculateOffset(IEnumerable<ReplicaId> allReplicaIds, ReplicaId ownReplicaId)
        => allReplicaIds
            .Select(s => s)
            .Order()
            .Select((id, i) => new { Id = id, Index = i })
            .FirstOrDefault(a => a.Id == ownReplicaId)
            ?.Index;

    private void ClearNonRelevantStrikes(IReadOnlyList<StoredReplica> storedReplicas)
    {
        foreach (var strikeKey in _strikes.Keys.ToList())
            if (storedReplicas.All(sr => sr != strikeKey))
                _strikes.Remove(strikeKey);
    }
    
    private void AddNewStrikes(IReadOnlyList<StoredReplica> storedReplicas)
    {
        foreach (var storedReplica in storedReplicas)
            _strikes.TryAdd(storedReplica, 0);
    }
    
    private void IncrementStrikesCount()
    {
        foreach (var storedReplica in _strikes.Keys)
            _strikes[storedReplica]++;
    }

    private async Task DeleteStrikedOutReplicas()
    {
        foreach (var (storedReplica, _) in _strikes.Where(kv => kv.Value >= 2))
        {
            var strikedOutId = storedReplica.ReplicaId;
            await ReplicaStore.Delete(strikedOutId);
            await functionStore.RescheduleCrashedFunctions(strikedOutId);
            _ = Task
                .Delay(TimeSpan.FromSeconds(5))
                .ContinueWith(_ => { if (!_disposed) functionStore.RescheduleCrashedFunctions(strikedOutId); });
            
            _strikes.Remove(storedReplica);
        }
    }
    
    public IReadOnlyDictionary<StoredReplica, int> Strikes => _strikes;

    public void Stop() => Dispose();
    public void Dispose() => _disposed = true;
}