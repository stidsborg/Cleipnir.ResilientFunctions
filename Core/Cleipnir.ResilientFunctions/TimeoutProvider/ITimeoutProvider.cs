using System;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.TimeoutProvider;

public interface ITimeoutProvider
{
    Task<CancellationToken> RegisterTimeout(FunctionId functionId, string timeoutId, DateTime expiry);
    Task RemoveTimeout(FunctionId functionId, string timeoutId);
}