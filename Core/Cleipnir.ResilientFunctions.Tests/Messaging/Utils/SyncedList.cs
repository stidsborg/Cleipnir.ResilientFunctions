﻿using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Cleipnir.ResilientFunctions.Tests.Messaging.Utils
{
    public class SyncedList<T> : IList<T>
    {
        private readonly List<T> _list = new();
        
        public IEnumerator<T> GetEnumerator()
        {
            lock (_list)
                return _list.ToList().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public void Add(T item)
        {
            lock (_list)
                _list.Add(item);
        }

        public void Clear()
        {
            lock (_list)
                _list.Clear();
        }

        public bool Contains(T item)
        {
            lock (_list)
                return _list.Contains(item);
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            lock (_list)
                _list.CopyTo(array, arrayIndex);
        }

        public bool Remove(T item)
        {
            lock (_list)
                return _list.Remove(item);
        }

        public int Count
        {
            get
            {
                lock (_list)
                    return _list.Count;
            }
        }

        public bool IsReadOnly => false;

        public int IndexOf(T item)
        {
            lock (_list)
                return _list.IndexOf(item);
        }

        public void Insert(int index, T item)
        {
            lock (_list)
                _list.Insert(index, item);
        }

        public void RemoveAt(int index)
        {
            lock (_list)
                _list.RemoveAt(index);
        }

        public T this[int index]
        {
            get
            {
                lock (_list)
                    return _list[index];
            }
            set
            {
                lock (_list)
                    _list[index] = value;
            }
        }
    }
}