using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Watchdogs.Invocation;

internal delegate Task<Return<object?>> WrappedInnerFunc(object param, RScrapbook? scrapbook);