using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Storage;

public class MessageBatcher<TEntity>(Func<StoredId, List<TEntity>, Task> handleBatchFunc)
{
    private readonly Dictionary<StoredId, Tuple<TaskCompletionSource, List<TEntity>>> _batchesDict = new();
    private readonly Lock _lock = new();

    public async Task Handle(StoredId storedId, IReadOnlyList<TEntity> storedMessages)
    {
        if (storedMessages.Count == 0)
            return;
        
        TaskCompletionSource? nextTcs;
        List<TEntity> nextBatch = [];
        bool firstExecution = false;

        lock (_lock)
        {
            if (_batchesDict.TryGetValue(storedId, out var tuple))
            {
                //already executing handling function for stored id
                nextTcs = tuple.Item1;
                tuple.Item2.AddRange(storedMessages);
            }
            else
            {
                //first execution
                nextTcs = new TaskCompletionSource();
                nextBatch = new List<TEntity>();
                
                _batchesDict[storedId] = Tuple.Create(nextTcs, nextBatch);
                firstExecution = true;
            }
        }

        if (!firstExecution)
        {
            await nextTcs.Task;
            return;
        }

        try
        {
            await handleBatchFunc(storedId, storedMessages.ToList());
            lock (_lock)
            {
                var tuple = _batchesDict[storedId];
                if (tuple.Item2.Count == 0)
                    _batchesDict.Remove(storedId);
                else
                    _ = Task.Run(() => HandleNext(storedId));
            }
        }
        catch (Exception)
        {
            _ = Task.Run(() => HandleNext(storedId));
            throw;
        }
    }

    private async Task HandleNext(StoredId storedId)
    {
        while (true)
        {
            TaskCompletionSource tcs;
            List<TEntity> batch;

            lock (_lock)
            {
                var tuple = _batchesDict[storedId];
                tcs = tuple.Item1;
                batch = tuple.Item2;
                if (batch.Count == 0)
                {
                    _batchesDict.Remove(storedId);
                    return;
                }

                _batchesDict[storedId] = Tuple.Create(new TaskCompletionSource(), new List<TEntity>());
            }

            try
            {
                await handleBatchFunc(storedId, batch);
                tcs.SetResult();
            }
            catch (Exception e)
            {
                tcs.SetException(e);
            }
        }
    }
}