using System;
using Orleans.Runtime;
using Microsoft.Extensions.Logging;

namespace Orleans.Indexing
{
    internal enum IndexingErrorCode
    {
        /// <summary>
        /// Start of orlean servicebus errocodes
        /// </summary>
        OrleansIndexing = 1 << 19,
            Indexing_IdAllocationFailed = OrleansIndexing,
            Indexing_PrepareFailed      = OrleansIndexing + 1,
            Indexing_CommitFailed       = OrleansIndexing + 2,
    }

    internal static class LoggerExtensions
    {
        internal static void Debug(this ILogger logger, IndexingErrorCode errorCode, string format, params object[] args)
        {
            if (logger.IsEnabled(LogLevel.Debug)) logger.Debug((int)errorCode, format, args, null);
        }

        internal static void Trace(this ILogger logger, IndexingErrorCode errorCode, string format, params object[] args)
        {
            if (logger.IsEnabled(LogLevel.Trace)) logger.Trace((int)errorCode, format, args, null);
        }

        internal static void Info(this ILogger logger, IndexingErrorCode errorCode, string format, params object[] args)
        {
            if (logger.IsEnabled(LogLevel.Information)) logger.Info((int)errorCode, format, args, null);
        }

        internal static void Warn(this ILogger logger, IndexingErrorCode errorCode, string format, params object[] args)
        {
            if (logger.IsEnabled(LogLevel.Warning)) logger.Warn((int)errorCode, format, args, null);
        }

        internal static void Warn(this ILogger logger, IndexingErrorCode errorCode, string message, Exception exception)
        {
            if (logger.IsEnabled(LogLevel.Warning)) logger.Warn((int)errorCode,  message, new object[] { }, exception);
        }

        internal static void Error(this ILogger logger, IndexingErrorCode errorCode, string message, Exception exception = null)
        {
            if (logger.IsEnabled(LogLevel.Error)) logger.Error((int)errorCode, message, exception);
        }
    }
}
