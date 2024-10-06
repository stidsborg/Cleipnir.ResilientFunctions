using System;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Reactive.Utilities;

public class EitherOrNone<T1, T2>
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

    public enum Value
    {
        None,
        First,
        Second
    }

    public static EitherOrNone<T1, T2> CreateNone() => new(Value.None, first: default!, second: default!);
    public static EitherOrNone<T1, T2> CreateFirst(T1 first) => new(Value.First, first, second: default!);
    public static EitherOrNone<T1, T2> CreateSecond(T2 second) => new(Value.Second, first: default!, second);

    public static EitherOrNone<T1, T2> CreateFromEither(Either<T1, T2> either) =>
        either.HasFirst ? CreateFirst(either.First) : CreateSecond(either.Second);
}