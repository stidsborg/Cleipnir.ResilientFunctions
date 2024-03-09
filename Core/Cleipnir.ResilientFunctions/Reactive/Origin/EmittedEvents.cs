﻿using System;
using System.Collections.Generic;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Reactive.Origin;

internal class EmittedEvents
{
    private EmittedEvent[] _backingArray = new EmittedEvent[8];
    private int _count;

    private InterruptCount _interruptCount = new(0);
    
    private readonly object _sync = new();

    public InterruptCount InterruptCount
    {
        get
        {
            lock (_sync)
                return _interruptCount;
        }

        set
        {
            lock (_sync)
                _interruptCount = value;
        }
    }

    public int Count
    {
        get
        {
            lock (_sync) return _count;
        }
    }

    public void Append(EmittedEvent emittedEvent)
    {
        lock (_sync)
        {
            if (_backingArray.Length == _count)
            {
                var prev = _backingArray;
                var curr = new EmittedEvent[prev.Length * 2];
                Array.Copy(sourceArray: prev, destinationArray: curr, length: prev.Length);
                _backingArray = curr;
            }

            _backingArray[_count] = emittedEvent;
            _count++;
        }
    }

    public void Append(IEnumerable<EmittedEvent> emittedEvents)
    {
        lock (_sync)
        {
            foreach (var emittedEvent in emittedEvents)
            {
                if (_backingArray.Length == _count)
                {
                    var prev = _backingArray;
                    var curr = new EmittedEvent[prev.Length * 2];
                    Array.Copy(sourceArray: prev, destinationArray: curr, length: prev.Length);
                    _backingArray = curr;
                }

                _backingArray[_count] = emittedEvent;
                _count++;
            }
        }
    }

    public Span<EmittedEvent> GetEvents(int skip)
    {
        lock (_sync)
            return _backingArray.AsSpan(start: skip, length: _count - skip);
    }
}