using System;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

public static class FuncExtensions
{
    public static Func<T> ToFunc<T>(this T t) => () => t;
}