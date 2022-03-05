using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Watchdogs.Invocation;

internal delegate Task<Return> InnerAction(object param, RScrapbook? scrapbook);