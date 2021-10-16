using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions
{
    public delegate Task<TReturn> RFunction<in TParam, TReturn>(TParam param, FunctionInstanceId? id = null);

    public static class RFunctionExtensions
    {
        public static Func<TParam, Task<TReturn>> ToFunc<TParam, TReturn>(this RFunction<TParam, TReturn> rf)
            => param => rf(param);
    }
}