using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.Storage;

public class MessageBatcher
{
    public StoredType StoredType { get; }

    public TaskCompletionSource? _current;
    private List<StoredMessage> _batch = new List<StoredMessage>();
    private readonly Lock _lock = new Lock();

    private Func<StoredId, IEnumerable<StoredMessage>, Task> _appendMessages;

    public MessageBatcher(StoredType storedType, Func<StoredId, IEnumerable<StoredMessage>, Task> appendMessages)
    {
        StoredType = storedType;
        _appendMessages = appendMessages;
    }

    public async Task AppendMessage(StoredId storedId, IEnumerable<StoredMessage> storedMessages)
    {
        TaskCompletionSource? tcs = null;
        lock (_lock)
        {
            if (_current != null)
            {
                _batch.AddRange(storedMessages);
                tcs = _current;
            }
            else
            {
                _current = new TaskCompletionSource();
                _batch = storedMessages.ToList();
            }
        }

        if (tcs != null)
            await tcs.Task;
        else
        {
            while (true)
            {
                List<StoredMessage> batch;
                TaskCompletionSource? current;
            
                lock (_lock)
                {
                    batch = _batch;
                    _batch = new List<StoredMessage>();
                    current = _current;
                    if (batch.Count == 0)
                    {
                        _current = null;
                        return;
                    } 
                    
                    _current = new TaskCompletionSource();
                }

                try
                {
                    await _appendMessages(storedId, batch);
                }
                catch (Exception e)
                {
                    current?.SetException(e);
                    lock (_lock)
                    {
                        _current?.SetException(e);
                        _current = null;
                        _batch.Clear();
                    }
                    
                    throw;
                }
                
                current.SetResult();
            }
        }
    }
}