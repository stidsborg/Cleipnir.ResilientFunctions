using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions;

public delegate Task Schedule<TParam>(TParam param);