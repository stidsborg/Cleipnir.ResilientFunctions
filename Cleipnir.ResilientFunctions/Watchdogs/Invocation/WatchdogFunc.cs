using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Watchdogs.Invocation;

internal delegate Task<Return<object?>> WatchdogFunc(object param, RScrapbook? scrapbook);