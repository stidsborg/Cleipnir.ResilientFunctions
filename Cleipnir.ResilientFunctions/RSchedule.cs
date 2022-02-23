using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions;

public delegate Task Schedule<in TParam>(string functionInstanceId, TParam param) where TParam : notnull;