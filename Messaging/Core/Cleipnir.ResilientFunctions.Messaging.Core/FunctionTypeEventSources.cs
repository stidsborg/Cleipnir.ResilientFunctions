﻿using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging.Core.Serialization;

namespace Cleipnir.ResilientFunctions.Messaging.Core;

public class FunctionTypeEventSources
{
    private readonly IEventStore _eventStore;
    private readonly FunctionTypeId _functionTypeId;
    private readonly TimeSpan? _pullFrequency;
    private readonly IEventSerializer? _eventSerializer;

    public FunctionTypeEventSources(
        IEventStore eventStore, 
        FunctionTypeId functionTypeId, 
        TimeSpan? pullFrequency,
        IEventSerializer? eventSerializer)
    {
        _eventStore = eventStore;
        _functionTypeId = functionTypeId;
        _pullFrequency = pullFrequency;
        _eventSerializer = eventSerializer;
    }

    public Task<EventSource> Get(
        string functionInstanceId, 
        TimeSpan? pullFrequency = null,
        IEventSerializer? eventSerializer = null
    ) => Get(new FunctionInstanceId(functionInstanceId), pullFrequency, eventSerializer);    
    
    public async Task<EventSource> Get(
        FunctionInstanceId functionInstanceId, 
        TimeSpan? pullFrequency = null,
        IEventSerializer? eventSerializer = null)
    {
        var eventWriter = new EventSourceWriter(
            _functionTypeId,
            _eventStore,
            eventSerializer ?? _eventSerializer,
            reInvoke: null
        ).For(functionInstanceId);
        
        var eventSource = new EventSource(
            new FunctionId(_functionTypeId, functionInstanceId),
            _eventStore,
            eventWriter,
            pullFrequency ?? _pullFrequency,
            eventSerializer ?? _eventSerializer
        );
        await eventSource.Initialize();

        return eventSource;
    }

    public EventSourceWriter CreateWriter()
        => new EventSourceWriter(
            _functionTypeId,
            _eventStore,
            _eventSerializer,
            reInvoke: null
        );
    
    public EventSourceWriter CreateWriter<TParam, TScrapbook, TReturn>(RFunc<TParam, TScrapbook, TReturn> rFunc) 
        where TParam : notnull where TScrapbook : RScrapbook, new()
     => new EventSourceWriter(
            _functionTypeId,
            _eventStore,
            _eventSerializer,
            reInvoke: functionInstanceId => rFunc.ReInvoke(
                functionInstanceId.Value, 
                expectedStatuses: new[] { Status.Postponed }
            )
        );
    
    public EventSourceWriter CreateWriter<TParam, TScrapbook>(RAction<TParam, TScrapbook> rAction) 
        where TParam : notnull where TScrapbook : RScrapbook, new()
        => new EventSourceWriter(
            _functionTypeId,
            _eventStore,
            _eventSerializer,
            reInvoke: functionInstanceId => rAction.ReInvoke(
                functionInstanceId.Value, 
                expectedStatuses: new[] { Status.Postponed }
            )
        );
}