using System;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;

namespace Cleipnir.ResilientFunctions.Reactive.Utilities;

public class EitherOrNone<T1, T2> : ICustomSerializable
{
    public Value ValueSpecified { get; }

    public bool HasNone => ValueSpecified == Value.None;
    
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

    private EitherOrNone(Value valueSpecifiedSpecified, T1 first, T2 second)
    {
        ValueSpecified = valueSpecifiedSpecified;
        _first = first;
        _second = second;
    }

    public T Match<T>(Func<T1, T> first, Func<T2, T> second, Func<T> none)
    {
        return ValueSpecified switch
        {
            Value.None => none(),
            Value.First => first(First),
            Value.Second => second(Second),
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    public void Do(Action<T1> first, Action<T2> second, Action none)
    {
        switch (ValueSpecified)
        {
            case Value.None:
                none();
                break;
            case Value.First:
                first(First);
                break;
            case Value.Second:
                second(Second);
                break;
        }
    }
    
    public async Task Do(Func<T1, Task> first, Func<T2, Task> second, Func<Task> none)
    {
        switch (ValueSpecified)
        {
            case Value.None:
                await none();
                break;
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
            Value.None => null, 
            Value.First => First,
            Value.Second => Second,
            _ => throw new ArgumentOutOfRangeException()
        };
    }
    
    public T? As<T>() where T : T1, T2 
    {
        return ValueSpecified switch
        {
            Value.None => default,
            Value.First => (T) First!,
            Value.Second => (T) Second!,
            _ => throw new ArgumentOutOfRangeException()
        };
    } 

    public enum Value : byte
    {
        None = 0,
        First = 1,
        Second = 2
    }

    public static EitherOrNone<T1, T2> CreateNone() => new(Value.None, first: default!, second: default!);
    public static EitherOrNone<T1, T2> CreateFirst(T1 first) => new(Value.First, first, second: default!);
    public static EitherOrNone<T1, T2> CreateSecond(T2 second) => new(Value.Second, first: default!, second);

    public static EitherOrNone<T1, T2> CreateFromEither(Either<T1, T2> either) =>
        either.HasFirst ? CreateFirst(either.First) : CreateSecond(either.Second);

    public byte[] Serialize(ISerializer serializer)
    {
        var tuple = Tuple.Create((byte)ValueSpecified, _first, _second);
        return serializer.Serialize(tuple);
    }

    public static object Deserialize(byte[] bytes, ISerializer serializer)
    {
        var (valueSpecified, t1, t2) = (Tuple<byte, T1, T2>)serializer.Deserialize(bytes, typeof(Tuple<byte, T1, T2>));
        return new EitherOrNone<T1, T2>((Value) valueSpecified, t1, t2);
    }
}

public class EitherOrNone<T1, T2, T3> : ICustomSerializable
{
    public Value ValueSpecified { get; }

    public bool HasNone => ValueSpecified == Value.None;

    public bool HasValue => !HasNone;
    
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
            if (ValueSpecified != Value.Third) throw new InvalidOperationException("Second value has not been set");
            return _third;
        }
    }

    private EitherOrNone(Value valueSpecifiedSpecified, T1 first, T2 second, T3 third)
    {
        ValueSpecified = valueSpecifiedSpecified;
        _first = first;
        _second = second;
        _third = third;
    }
    

    public T Match<T>(Func<T1, T> first, Func<T2, T> second, Func<T3, T> third, Func<T> none)
    {
        return ValueSpecified switch
        {
            Value.None => none(),
            Value.First => first(First),
            Value.Second => second(Second),
            Value.Third => third(Third),
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    public void Do(Action<T1> first, Action<T2> second, Action<T3> third, Action none)
    {
        switch (ValueSpecified)
        {
            case Value.None:
                none();
                break;
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
    
    public async Task Do(Func<T1, Task> first, Func<T2, Task> second, Func<T3, Task> third, Func<Task> none)
    {
        switch (ValueSpecified)
        {
            case Value.None:
                await none();
                break;
            case Value.First:
                await first(First);
                break;
            case Value.Second:
                await second(Second);
                break;
            case Value.Third:
                await third(Third);
                break;
        }
    }
    
    public object? AsObject()
    {
        return ValueSpecified switch
        {
            Value.None => null, 
            Value.First => First,
            Value.Second => Second,
            Value.Third => Third,
            _ => throw new ArgumentOutOfRangeException()
        };
    }
    
    public T? As<T>() where T : T1, T2, T3 
    {
        return ValueSpecified switch
        {
            Value.None => default,
            Value.First => (T) First!,
            Value.Second => (T) Second!,
            Value.Third => (T) Third!,
            _ => throw new ArgumentOutOfRangeException()
        };
    } 

    public enum Value
    {
        None = 0,
        First = 1,
        Second = 2,
        Third = 3
    }

    public static EitherOrNone<T1, T2, T3> CreateNone() => new(Value.None, first: default!, second: default!, third: default!);
    public static EitherOrNone<T1, T2, T3> CreateFirst(T1 first) => new(Value.First, first, second: default!, third: default!);
    public static EitherOrNone<T1, T2, T3> CreateSecond(T2 second) => new(Value.Second, first: default!, second, third: default!);
    public static EitherOrNone<T1, T2, T3> CreateThird(T3 third) => new(Value.Third, first: default!, second: default!, third);
    public static EitherOrNone<T1, T2, T3> CreateFromEither(Either<T1, T2, T3> either) => either.Match(CreateFirst, CreateSecond, CreateThird);
    
    public byte[] Serialize(ISerializer serializer)
    {
        var tuple = Tuple.Create((byte)ValueSpecified, _first, _second, _third);
        return serializer.Serialize(tuple);
    }

    public static object Deserialize(byte[] bytes, ISerializer serializer)
    {
        var (valueSpecified, t1, t2, t3) = (Tuple<byte, T1, T2, T3>)serializer.Deserialize(bytes, typeof(Tuple<byte, T1, T2, T3>));
        return new EitherOrNone<T1, T2, T3>((Value) valueSpecified, t1, t2, t3);
    }
}