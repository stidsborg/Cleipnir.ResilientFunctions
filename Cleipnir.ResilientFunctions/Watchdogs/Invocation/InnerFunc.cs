using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Watchdogs.Invocation;

internal delegate Task<Return<TReturn>> InnerFunc<TReturn>(object param, RScrapbook? scrapbook);
