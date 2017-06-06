// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;

// ReSharper disable once CheckNamespace
namespace Microsoft.EntityFrameworkCore.Storage
{
    public static class RelationalExecutionStrategyExtensions
    {
        /// <summary>
        ///     Executes the specified operation in a transaction.
        /// </summary>
        /// <param name="strategy"> The strategy that will be used for the execution. </param>
        /// <param name="isolationLevel"> The isolation level to use for the transaction. </param>
        /// <param name="operation">
        ///     A delegate representing an executable operation.
        /// </param>
        /// <param name="verifySucceeded">
        ///     A delegate that tests whether the operation succeeded even though an exception was thrown when the
        ///     transaction was being committed.
        /// </param>
        /// <param name="context"> The context that will be used to start the transaction. </param>
        /// <typeparam name="TContext"> The type of the supplied context instance. </typeparam>
        /// <exception cref="RetryLimitExceededException">
        ///     Thrown if the operation has not succeeded after the configured number of retries.
        /// </exception>
        public static void ExecuteInTransaction<TContext>(
            [NotNull] this IExecutionStrategy strategy,
            IsolationLevel isolationLevel,
            [NotNull] Action<TContext> operation,
            [NotNull] Func<TContext, bool> verifySucceeded,
            [NotNull] TContext context)
            where TContext : DbContext
            => strategy.ExecuteInTransaction<TContext, object>(isolationLevel, (c, s) => operation(c), (c, s) => verifySucceeded(c), null, context);

        /// <summary>
        ///     Executes the specified asynchronous operation in a transaction.
        /// </summary>
        /// <param name="strategy"> The strategy that will be used for the execution. </param>
        /// <param name="isolationLevel"> The isolation level to use for the transaction. </param>
        /// <param name="operation">
        ///     A function that returns a started task.
        /// </param>
        /// <param name="verifySucceeded">
        ///     A delegate that tests whether the operation succeeded even though an exception was thrown when the
        ///     transaction was being committed.
        /// </param>
        /// <param name="context"> The context that will be used to start the transaction. </param>
        /// <typeparam name="TContext"> The type of the supplied context instance. </typeparam>
        /// <returns>
        ///     A task that will run to completion if the original task completes successfully (either the
        ///     first time or after retrying transient failures). If the task fails with a non-transient error or
        ///     the retry limit is reached, the returned task will become faulted and the exception must be observed.
        /// </returns>
        /// <exception cref="RetryLimitExceededException">
        ///     Thrown if the operation has not succeeded after the configured number of retries.
        /// </exception>
        public static Task ExecuteInTransactionAsync<TContext>(
            [NotNull] this IExecutionStrategy strategy,
            IsolationLevel isolationLevel,
            [NotNull] Func<TContext, Task> operation,
            [NotNull] Func<TContext, Task<bool>> verifySucceeded,
            [NotNull] TContext context)
            where TContext : DbContext
            => strategy.ExecuteInTransactionAsync<TContext, object>(isolationLevel, (c, s, ct) => operation(c), (c, s, ct) => verifySucceeded(c), null, context, default(CancellationToken));

        /// <summary>
        ///     Executes the specified asynchronous operation in a transaction.
        /// </summary>
        /// <param name="strategy"> The strategy that will be used for the execution. </param>
        /// <param name="isolationLevel"> The isolation level to use for the transaction. </param>
        /// <param name="operation">
        ///     A function that returns a started task.
        /// </param>
        /// <param name="verifySucceeded">
        ///     A delegate that tests whether the operation succeeded even though an exception was thrown when the
        ///     transaction was being committed.
        /// </param>
        /// <param name="context"> The context that will be used to start the transaction. </param>
        /// <param name="cancellationToken">
        ///     A cancellation token used to cancel the retry operation, but not operations that are already in flight
        ///     or that already completed successfully.
        /// </param>
        /// <typeparam name="TContext"> The type of the supplied context instance. </typeparam>
        /// <returns>
        ///     A task that will run to completion if the original task completes successfully (either the
        ///     first time or after retrying transient failures). If the task fails with a non-transient error or
        ///     the retry limit is reached, the returned task will become faulted and the exception must be observed.
        /// </returns>
        /// <exception cref="RetryLimitExceededException">
        ///     Thrown if the operation has not succeeded after the configured number of retries.
        /// </exception>
        public static Task ExecuteInTransactionAsync<TContext>(
            [NotNull] this IExecutionStrategy strategy,
            IsolationLevel isolationLevel,
            [NotNull] Func<TContext, CancellationToken, Task> operation,
            [NotNull] Func<TContext, CancellationToken, Task<bool>> verifySucceeded,
            [NotNull] TContext context,
            CancellationToken cancellationToken = default(CancellationToken))
            where TContext : DbContext
            => strategy.ExecuteInTransactionAsync<TContext, object>(isolationLevel, (c, s, ct) => operation(c, ct), (c, s, ct) => verifySucceeded(c, ct), null, context, cancellationToken);

        /// <summary>
        ///     Executes the specified operation in a transaction and returns the result.
        /// </summary>
        /// <param name="strategy"> The strategy that will be used for the execution. </param>
        /// <param name="isolationLevel"> The isolation level to use for the transaction. </param>
        /// <param name="operation">
        ///     A delegate representing an executable operation that returns the result of type <typeparamref name="TResult" />.
        /// </param>
        /// <param name="verifySucceeded">
        ///     A delegate that tests whether the operation succeeded even though an exception was thrown when the
        ///     transaction was being committed.
        /// </param>
        /// <param name="context"> The context that will be used to start the transaction. </param>
        /// <typeparam name="TContext"> The type of the supplied context instance. </typeparam>
        /// <typeparam name="TResult"> The return type of <paramref name="operation" />. </typeparam>
        /// <returns> The result from the operation. </returns>
        /// <exception cref="RetryLimitExceededException">
        ///     Thrown if the operation has not succeeded after the configured number of retries.
        /// </exception>
        public static TResult ExecuteInTransaction<TContext, TResult>(
            [NotNull] this IExecutionStrategy strategy,
            IsolationLevel isolationLevel,
            [NotNull] Func<TContext, TResult> operation,
            [NotNull] Func<TContext, bool> verifySucceeded,
            [NotNull] TContext context)
            where TContext : DbContext
            => strategy.ExecuteInTransaction<TContext, object, TResult>(isolationLevel, (c, s) => operation(c), (c, s) => verifySucceeded(c), null, context);

        /// <summary>
        ///     Executes the specified asynchronous operation in a transaction and returns the result.
        /// </summary>
        /// <param name="strategy"> The strategy that will be used for the execution. </param>
        /// <param name="isolationLevel"> The isolation level to use for the transaction. </param>
        /// <param name="operation">
        ///     A function that returns a started task of type <typeparamref name="TResult" />.
        /// </param>
        /// <param name="verifySucceeded">
        ///     A delegate that tests whether the operation succeeded even though an exception was thrown when the
        ///     transaction was being committed.
        /// </param>
        /// <param name="context"> The context that will be used to start the transaction. </param>
        /// <param name="cancellationToken">
        ///     A cancellation token used to cancel the retry operation, but not operations that are already in flight
        ///     or that already completed successfully.
        /// </param>
        /// <typeparam name="TContext"> The type of the supplied context instance. </typeparam>
        /// <typeparam name="TResult"> The result type of the <see cref="Task{T}" /> returned by <paramref name="operation" />. </typeparam>
        /// <returns>
        ///     A task that will run to completion if the original task completes successfully (either the
        ///     first time or after retrying transient failures). If the task fails with a non-transient error or
        ///     the retry limit is reached, the returned task will become faulted and the exception must be observed.
        /// </returns>
        /// <exception cref="RetryLimitExceededException">
        ///     Thrown if the operation has not succeeded after the configured number of retries.
        /// </exception>
        public static Task<TResult> ExecuteInTransactionAsync<TContext, TResult>(
            [NotNull] this IExecutionStrategy strategy,
            IsolationLevel isolationLevel,
            [NotNull] Func<TContext, CancellationToken, Task<TResult>> operation,
            [NotNull] Func<TContext, CancellationToken, Task<bool>> verifySucceeded,
            [NotNull] TContext context,
            CancellationToken cancellationToken = default(CancellationToken))
            where TContext : DbContext
            => strategy.ExecuteInTransactionAsync<TContext, object, TResult>(isolationLevel, (c, s, ct) => operation(c, ct), (c, s, ct) => verifySucceeded(c, ct), null, context, cancellationToken);

        /// <summary>
        ///     Executes the specified operation in a transaction.
        /// </summary>
        /// <param name="strategy"> The strategy that will be used for the execution. </param>
        /// <param name="isolationLevel"> The isolation level to use for the transaction. </param>
        /// <param name="operation">
        ///     A delegate representing an executable operation.
        /// </param>
        /// <param name="verifySucceeded">
        ///     A delegate that tests whether the operation succeeded even though an exception was thrown when the
        ///     transaction was being committed.
        /// </param>
        /// <param name="state"> The state that will be passed to the operation. </param>
        /// <param name="context"> The context that will be used to start the transaction. </param>
        /// <typeparam name="TContext"> The type of the supplied context instance. </typeparam>
        /// <typeparam name="TState"> The type of the state. </typeparam>
        /// <exception cref="RetryLimitExceededException">
        ///     Thrown if the operation has not succeeded after the configured number of retries.
        /// </exception>
        public static void ExecuteInTransaction<TContext, TState>(
            [NotNull] this IExecutionStrategy strategy,
            IsolationLevel isolationLevel,
            [NotNull] Action<TContext, TState> operation,
            [NotNull] Func<TContext, TState, bool> verifySucceeded,
            [CanBeNull] TState state,
            [NotNull] TContext context)
            where TContext : DbContext
            => strategy.ExecuteInTransaction(isolationLevel,
                (c, s) =>
                    {
                        operation(c, s);
                        return true;
                    },
                verifySucceeded, state, context);

        /// <summary>
        ///     Executes the specified asynchronous operation in a transaction.
        /// </summary>
        /// <param name="strategy"> The strategy that will be used for the execution. </param>
        /// <param name="isolationLevel"> The isolation level to use for the transaction. </param>
        /// <param name="operation">
        ///     A function that returns a started task.
        /// </param>
        /// <param name="verifySucceeded">
        ///     A delegate that tests whether the operation succeeded even though an exception was thrown when the
        ///     transaction was being committed.
        /// </param>
        /// <param name="state"> The state that will be passed to the operation. </param>
        /// <param name="context"> The context that will be used to start the transaction. </param>
        /// <param name="cancellationToken">
        ///     A cancellation token used to cancel the retry operation, but not operations that are already in flight
        ///     or that already completed successfully.
        /// </param>
        /// <typeparam name="TContext"> The type of the supplied context instance. </typeparam>
        /// <typeparam name="TState"> The type of the state. </typeparam>
        /// <returns>
        ///     A task that will run to completion if the original task completes successfully (either the
        ///     first time or after retrying transient failures). If the task fails with a non-transient error or
        ///     the retry limit is reached, the returned task will become faulted and the exception must be observed.
        /// </returns>
        /// <exception cref="RetryLimitExceededException">
        ///     Thrown if the operation has not succeeded after the configured number of retries.
        /// </exception>
        public static Task ExecuteInTransactionAsync<TContext, TState>(
            [NotNull] this IExecutionStrategy strategy,
            IsolationLevel isolationLevel,
            [NotNull] Func<TContext, TState, CancellationToken, Task> operation,
            [NotNull] Func<TContext, TState, CancellationToken, Task<bool>> verifySucceeded,
            [CanBeNull] TState state,
            [NotNull] TContext context,
            CancellationToken cancellationToken = default(CancellationToken))
            where TContext : DbContext
            => strategy.ExecuteInTransactionAsync(isolationLevel,
                async (c, s, ct) =>
                    {
                        await operation(c, s, ct);
                        return true;
                    }, verifySucceeded, state, context, cancellationToken);

        /// <summary>
        ///     Executes the specified operation in a transaction and returns the result.
        /// </summary>
        /// <param name="strategy"> The strategy that will be used for the execution. </param>
        /// <param name="isolationLevel"> The isolation level to use for the transaction. </param>
        /// <param name="operation">
        ///     A delegate representing an executable operation that returns the result of type <typeparamref name="TResult" />.
        /// </param>
        /// <param name="verifySucceeded">
        ///     A delegate that tests whether the operation succeeded even though an exception was thrown when the
        ///     transaction was being committed.
        /// </param>
        /// <param name="state"> The state that will be passed to the operation. </param>
        /// <param name="context"> The context that will be used to start the transaction. </param>
        /// <typeparam name="TContext"> The type of the supplied context instance. </typeparam>
        /// <typeparam name="TState"> The type of the state. </typeparam>
        /// <typeparam name="TResult"> The return type of <paramref name="operation" />. </typeparam>
        /// <returns> The result from the operation. </returns>
        /// <exception cref="RetryLimitExceededException">
        ///     Thrown if the operation has not succeeded after the configured number of retries.
        /// </exception>
        public static TResult ExecuteInTransaction<TContext, TState, TResult>(
            [NotNull] this IExecutionStrategy strategy,
            IsolationLevel isolationLevel,
            [NotNull] Func<TContext, TState, TResult> operation,
            [NotNull] Func<TContext, TState, bool> verifySucceeded,
            [CanBeNull] TState state,
            [NotNull] TContext context)
            where TContext : DbContext
            => ExecutionStrategyExtensions.ExecuteInTransaction(strategy, operation, verifySucceeded, state, context, c => c.Database.BeginTransaction(isolationLevel));

        /// <summary>
        ///     Executes the specified asynchronous operation and returns the result.
        /// </summary>
        /// <param name="strategy"> The strategy that will be used for the execution. </param>
        /// <param name="isolationLevel"> The isolation level to use for the transaction. </param>
        /// <param name="operation">
        ///     A function that returns a started task of type <typeparamref name="TResult" />.
        /// </param>
        /// <param name="verifySucceeded">
        ///     A delegate that tests whether the operation succeeded even though an exception was thrown when the
        ///     transaction was being committed.
        /// </param>
        /// <param name="cancellationToken">
        ///     A cancellation token used to cancel the retry operation, but not operations that are already in flight
        ///     or that already completed successfully.
        /// </param>
        /// <param name="state"> The state that will be passed to the operation. </param>
        /// <param name="context"> The context that will be used to start the transaction. </param>
        /// <typeparam name="TContext"> The type of the supplied context instance. </typeparam>
        /// <typeparam name="TState"> The type of the state. </typeparam>
        /// <typeparam name="TResult"> The result type of the <see cref="Task{TResult}" /> returned by <paramref name="operation" />. </typeparam>
        /// <returns>
        ///     A task that will run to completion if the original task completes successfully (either the
        ///     first time or after retrying transient failures). If the task fails with a non-transient error or
        ///     the retry limit is reached, the returned task will become faulted and the exception must be observed.
        /// </returns>
        /// <exception cref="RetryLimitExceededException">
        ///     Thrown if the operation has not succeeded after the configured number of retries.
        /// </exception>
        public static Task<TResult> ExecuteInTransactionAsync<TContext, TState, TResult>(
            [NotNull] this IExecutionStrategy strategy,
            IsolationLevel isolationLevel,
            [NotNull] Func<TContext, TState, CancellationToken, Task<TResult>> operation,
            [NotNull] Func<TContext, TState, CancellationToken, Task<bool>> verifySucceeded,
            [CanBeNull] TState state,
            [NotNull] TContext context,
            CancellationToken cancellationToken = default(CancellationToken))
            where TContext : DbContext
            => ExecutionStrategyExtensions.ExecuteInTransactionAsync(
                strategy, operation, verifySucceeded, state, context, (c, ct) => c.Database.BeginTransactionAsync(isolationLevel, ct), cancellationToken);
    }
}
