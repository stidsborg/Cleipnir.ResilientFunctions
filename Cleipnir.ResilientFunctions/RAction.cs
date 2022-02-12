using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions;

public delegate Task<RResult> RAction<TParam>(TParam param)
    where TParam : notnull;