using System;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;

namespace Cleipnir.ResilientFunctions.Reactive.Utilities;

public class Either<T1, T2> : ICustomSerializable
{
    public Value ValueSpecified { get; }

    public bool HasFirst => ValueSpecified == Value.First;
    private readonly T1 _first;

    public T1 First
    {
        get
        {
            if (ValueSpecified != Value.First) throw new InvalidOperationException("First value has not been set");
            return _first;
        }
    }

    public bool HasSecond => ValueSpecified == Value.Second;
    private readonly T2 _second;

    public T2 Second
    {
        get
        {
            if (ValueSpecified != Value.Second) throw new InvalidOperationException("Second value has not been set");
            return _second;
        }
    }

    private Either(Value valueSpecifiedSpecified, T1 first, T2 second)
    {
        ValueSpecified = valueSpecifiedSpecified;
        _first = first;
        _second = second;
    }

    public T Match<T>(Func<T1, T> first, Func<T2, T> second)
    {
        return ValueSpecified switch
        {
            Value.First => first(First),
            Value.Second => second(Second),
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    public void Do(Action<T1> first, Action<T2> second)
    {
        switch (ValueSpecified)
        {
            case Value.First:
                first(First);
                break;
            case Value.Second:
                second(Second);
                break;
        }
    }
    
    public async Task Do(Func<T1, Task> first, Func<T2, Task> second)
    {
        switch (ValueSpecified)
        {
            case Value.First:
                await first(First);
                break;
            case Value.Second:
                await second(Second);
                break;
        }
    }
    
    public object? AsObject()
    {
        return ValueSpecified switch
        {
            Value.First => First,
            Value.Second => Second,
            _ => throw new ArgumentOutOfRangeException()
        };
    }
    
    public T As<T>() where T : T1, T2 
    {
        return ValueSpecified switch
        {
            Value.First => (T) First!,
            Value.Second => (T) Second!,
            _ => throw new ArgumentOutOfRangeException()
        };
    } 

    public enum Value : byte
    {
        First = 1,
        Second = 2
    }

    public static Either<T1, T2> CreateFirst(T1 first) => new(Value.First, first, second: default!);
    public static Either<T1, T2> CreateSecond(T2 second) => new(Value.Second, first: default!, second);
    
    public byte[] Serialize(ISerializer serializer)
    {
        var valueSpecified = (byte) ValueSpecified;
        byte[] valueBytes;
        if (ValueSpecified == Value.First)
            valueBytes = serializer.Serialize(_first);
        else if (ValueSpecified == Value.Second)
            valueBytes = serializer.Serialize(_second);
        else
            throw new SerializationException("Unexpected ValueSpecified-value");
        
        var bytes = new byte[valueBytes.Length + 1];
        bytes[0] = valueSpecified;
        valueBytes.CopyTo(bytes, index: 1);
        return bytes;
    }

    public static object Deserialize(byte[] bytes, ISerializer serializer)
    {
        if (bytes[0] == 1)
            return CreateFirst(serializer.Deserialize<T1>(bytes[1..]));
        if(bytes[0] == 2)
            return CreateSecond(serializer.Deserialize<T2>(bytes[1..]));
        
        throw new SerializationException("Unexpected byte value");
    }
}

public class Either<T1, T2, T3> : ICustomSerializable
{
    public Value ValueSpecified { get; }

    public bool HasFirst => ValueSpecified == Value.First;
    private readonly T1 _first;

    public T1 First
    {
        get
        {
            if (ValueSpecified != Value.First) throw new InvalidOperationException("First value has not been set");
            return _first;
        }
    }

    public bool HasSecond => ValueSpecified == Value.Second;
    private readonly T2 _second;

    public T2 Second
    {
        get
        {
            if (ValueSpecified != Value.Second) throw new InvalidOperationException("Second value has not been set");
            return _second;
        }
    }

    public bool HasThird => ValueSpecified == Value.Third;
    private readonly T3 _third;

    public T3 Third
    {
        get
        {
            if (ValueSpecified != Value.Third) throw new InvalidOperationException("Third value has not been set");
            return _third;
        }
    }

    private Either(Value valueSpecifiedSpecified, T1 first, T2 second, T3 third)
    {
        ValueSpecified = valueSpecifiedSpecified;
        _first = first;
        _second = second;
        _third = third;
    }

    public T Match<T>(Func<T1, T> first, Func<T2, T> second, Func<T3, T> third)
    {
        return ValueSpecified switch
        {
            Value.First => first(First),
            Value.Second => second(Second),
            Value.Third => third(Third),
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    public void Do(Action<T1> first, Action<T2> second, Action<T3> third)
    {
        switch (ValueSpecified)
        {
            case Value.First:
                first(First);
                break;
            case Value.Second:
                second(Second);
                break;
            case Value.Third:
                third(Third);
                break;
        }
    }

    public object? AsObject()
    {
        return ValueSpecified switch
        {
            Value.First => First,
            Value.Second => Second,
            Value.Third => Third,
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    public T As<T>() where T : T1, T2, T3 
    {
        return ValueSpecified switch
        {
            Value.First => (T) First!,
            Value.Second => (T) Second!,
            Value.Third => (T) Third!,
            _ => throw new ArgumentOutOfRangeException()
        };
    } 

    public enum Value : byte
    {
        First = 1,
        Second = 2,
        Third = 3
    }

    public static Either<T1, T2, T3> CreateFirst(T1 first) => new(Value.First, first, second: default!, third: default!);
    public static Either<T1, T2, T3> CreateSecond(T2 second) => new(Value.Second, first: default!, second, third: default!);
    public static Either<T1, T2, T3> CreateThird(T3 third) => new(Value.Third, first: default!, second: default!, third);
    public byte[] Serialize(ISerializer serializer)
    {
        var valueSpecified = (byte) ValueSpecified;
        byte[] valueBytes;
        if (ValueSpecified == Value.First)
            valueBytes = serializer.Serialize(_first);
        else if (ValueSpecified == Value.Second)
            valueBytes = serializer.Serialize(_second);
        else if (ValueSpecified == Value.Third)
            valueBytes = serializer.Serialize(_third);
        else
            throw new SerializationException("Unexpected ValueSpecified-value");
        
        var bytes = new byte[valueBytes.Length + 1];
        bytes[0] = valueSpecified;
        valueBytes.CopyTo(bytes, index: 1);
        return bytes;
    }

    public static object Deserialize(byte[] bytes, ISerializer serializer)
    {
        if (bytes[0] == 1)
            return CreateFirst(serializer.Deserialize<T1>(bytes[1..]));
        if(bytes[0] == 2)
            return CreateSecond(serializer.Deserialize<T2>(bytes[1..]));
        if(bytes[0] == 3)
            return CreateThird(serializer.Deserialize<T3>(bytes[1..]));

        throw new SerializationException("Unexpected byte value");
    }
}