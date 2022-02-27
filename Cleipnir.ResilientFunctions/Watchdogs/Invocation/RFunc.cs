using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Watchdogs.Invocation;

internal delegate Task<Return<TReturn>> RFunc<TReturn>(object param, RScrapbook? scrapbook);
