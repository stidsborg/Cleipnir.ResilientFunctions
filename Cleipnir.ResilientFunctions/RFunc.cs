using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions;

public delegate Task<RResult<TResult>> RFunc<TParam, TResult>(TParam param)
    where TParam : notnull where TResult : notnull;
    