// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Utilities;

// ReSharper disable once CheckNamespace
namespace Microsoft.EntityFrameworkCore
{
    /// <summary>
    ///     Extension methods for <see cref="IExecutionStrategy" />
    /// </summary>
    public static class ExecutionStrategyExtensions
    {
        /// <summary>
        ///     Executes the specified operation.
        /// </summary>
        /// <param name="strategy">The strategy that will be used for the execution.</param>
        /// <param name="operation">A delegate representing an executable operation that doesn't return any results.</param>
        public static void Execute(
            [NotNull] this IExecutionStrategy strategy,
            [NotNull] Action operation)
        {
            Check.NotNull(operation, nameof(operation));

            strategy.Execute(operationScoped =>
                {
                    operationScoped();
                    return true;
                }, operation);
        }

        /// <summary>
        ///     Executes the specified operation and returns the result.
        /// </summary>
        /// <param name="strategy">The strategy that will be used for the execution.</param>
        /// <param name="operation">
        ///     A delegate representing an executable operation that returns the result of type <typeparamref name="TResult" />.
        /// </param>
        /// <typeparam name="TResult">The return type of <paramref name="operation" />.</typeparam>
        /// <returns>The result from the operation.</returns>
        public static TResult Execute<TResult>(
            [NotNull] this IExecutionStrategy strategy,
            [NotNull] Func<TResult> operation)
        {
            Check.NotNull(operation, nameof(operation));

            return strategy.Execute(operationScoped => operationScoped(), operation);
        }

        /// <summary>
        ///     Executes the specified operation.
        /// </summary>
        /// <param name="strategy">The strategy that will be used for the execution.</param>
        /// <param name="operation">A delegate representing an executable operation that doesn't return any results.</param>
        /// <param name="state">The state that will be passed to the operation.</param>
        /// <typeparam name="TState">The type of the state.</typeparam>
        public static void Execute<TState>(
            [NotNull] this IExecutionStrategy strategy,
            [NotNull] Action<TState> operation,
            [CanBeNull] TState state)
        {
            Check.NotNull(operation, nameof(operation));

            strategy.Execute(s =>
                {
                    s.operation(s.state);
                    return true;
                }, new { operation, state });
        }

        /// <summary>
        ///     Executes the specified asynchronous operation.
        /// </summary>
        /// <param name="strategy">The strategy that will be used for the execution.</param>
        /// <param name="operation">A function that returns a started task.</param>
        /// <returns>
        ///     A task that will run to completion if the original task completes successfully (either the
        ///     first time or after retrying transient failures). If the task fails with a non-transient error or
        ///     the retry limit is reached, the returned task will become faulted and the exception must be observed.
        /// </returns>
        public static Task ExecuteAsync(
            [NotNull] this IExecutionStrategy strategy,
            [NotNull] Func<Task> operation)
        {
            Check.NotNull(operation, nameof(operation));

            return strategy.ExecuteAsync(async (operationScoped, ct) =>
                {
                    await operationScoped();
                    return true;
                }, operation, default(CancellationToken));
        }

        /// <summary>
        ///     Executes the specified asynchronous operation.
        /// </summary>
        /// <param name="strategy">The strategy that will be used for the execution.</param>
        /// <param name="operation">A function that returns a started task.</param>
        /// <param name="cancellationToken">
        ///     A cancellation token used to cancel the retry operation, but not operations that are already in flight
        ///     or that already completed successfully.
        /// </param>
        /// <returns>
        ///     A task that will run to completion if the original task completes successfully (either the
        ///     first time or after retrying transient failures). If the task fails with a non-transient error or
        ///     the retry limit is reached, the returned task will become faulted and the exception must be observed.
        /// </returns>
        public static Task ExecuteAsync(
            [NotNull] this IExecutionStrategy strategy,
            [NotNull] Func<CancellationToken, Task> operation,
            CancellationToken cancellationToken)
        {
            Check.NotNull(operation, nameof(operation));

            return strategy.ExecuteAsync(async (operationScoped, ct) =>
                {
                    await operationScoped(ct);
                    return true;
                }, operation, cancellationToken);
        }

        /// <summary>
        ///     Executes the specified asynchronous operation and returns the result.
        /// </summary>
        /// <param name="strategy">The strategy that will be used for the execution.</param>
        /// <param name="operation">
        ///     A function that returns a started task of type <typeparamref name="TResult" />.
        /// </param>
        /// <typeparam name="TResult">
        ///     The result type of the <see cref="Task{T}" /> returned by <paramref name="operation" />.
        /// </typeparam>
        /// <returns>
        ///     A task that will run to completion if the original task completes successfully (either the
        ///     first time or after retrying transient failures). If the task fails with a non-transient error or
        ///     the retry limit is reached, the returned task will become faulted and the exception must be observed.
        /// </returns>
        public static Task<TResult> ExecuteAsync<TResult>(
            [NotNull] this IExecutionStrategy strategy,
            [NotNull] Func<Task<TResult>> operation)
        {
            Check.NotNull(operation, nameof(operation));

            return strategy.ExecuteAsync((operationScoped, ct) => operationScoped(), operation, default(CancellationToken));
        }

        /// <summary>
        ///     Executes the specified asynchronous operation and returns the result.
        /// </summary>
        /// <param name="strategy">The strategy that will be used for the execution.</param>
        /// <param name="operation">
        ///     A function that returns a started task of type <typeparamref name="TResult" />.
        /// </param>
        /// <param name="cancellationToken">
        ///     A cancellation token used to cancel the retry operation, but not operations that are already in flight
        ///     or that already completed successfully.
        /// </param>
        /// <typeparam name="TResult">
        ///     The result type of the <see cref="Task{T}" /> returned by <paramref name="operation" />.
        /// </typeparam>
        /// <returns>
        ///     A task that will run to completion if the original task completes successfully (either the
        ///     first time or after retrying transient failures). If the task fails with a non-transient error or
        ///     the retry limit is reached, the returned task will become faulted and the exception must be observed.
        /// </returns>
        public static Task<TResult> ExecuteAsync<TResult>(
            [NotNull] this IExecutionStrategy strategy,
            [NotNull] Func<CancellationToken, Task<TResult>> operation,
            CancellationToken cancellationToken)
        {
            Check.NotNull(operation, nameof(operation));

            return strategy.ExecuteAsync((operationScoped, ct) => operationScoped(ct), operation, cancellationToken);
        }

        /// <summary>
        ///     Executes the specified asynchronous operation.
        /// </summary>
        /// <param name="strategy">The strategy that will be used for the execution.</param>
        /// <param name="operation">A function that returns a started task.</param>
        /// <param name="state">The state that will be passed to the operation.</param>
        /// <typeparam name="TState">The type of the state.</typeparam>
        /// <returns>
        ///     A task that will run to completion if the original task completes successfully (either the
        ///     first time or after retrying transient failures). If the task fails with a non-transient error or
        ///     the retry limit is reached, the returned task will become faulted and the exception must be observed.
        /// </returns>
        public static Task ExecuteAsync<TState>(
            [NotNull] this IExecutionStrategy strategy,
            [NotNull] Func<TState, Task> operation,
            [CanBeNull] TState state)
        {
            Check.NotNull(operation, nameof(operation));

            return strategy.ExecuteAsync(async (t, ct) =>
                {
                    await t.operation(t.state);
                    return true;
                }, new { operation, state }, default(CancellationToken));
        }

        /// <summary>
        ///     Executes the specified asynchronous operation.
        /// </summary>
        /// <param name="strategy">The strategy that will be used for the execution.</param>
        /// <param name="operation">A function that returns a started task.</param>
        /// <param name="cancellationToken">
        ///     A cancellation token used to cancel the retry operation, but not operations that are already in flight
        ///     or that already completed successfully.
        /// </param>
        /// <param name="state">The state that will be passed to the operation.</param>
        /// <typeparam name="TState">The type of the state.</typeparam>
        /// <returns>
        ///     A task that will run to completion if the original task completes successfully (either the
        ///     first time or after retrying transient failures). If the task fails with a non-transient error or
        ///     the retry limit is reached, the returned task will become faulted and the exception must be observed.
        /// </returns>
        public static Task ExecuteAsync<TState>(
            [NotNull] this IExecutionStrategy strategy,
            [NotNull] Func<TState, CancellationToken, Task> operation,
            [CanBeNull] TState state,
            CancellationToken cancellationToken)
        {
            Check.NotNull(operation, nameof(operation));

            return strategy.ExecuteAsync(async (t, ct) =>
                {
                    await t.operation(t.state, ct);
                    return true;
                }, new { operation, state }, cancellationToken);
        }

        /// <summary>
        ///     Executes the specified asynchronous operation and returns the result.
        /// </summary>
        /// <param name="strategy">The strategy that will be used for the execution.</param>
        /// <param name="operation">
        ///     A function that returns a started task of type <typeparamref name="TResult" />.
        /// </param>
        /// <param name="state">The state that will be passed to the operation.</param>
        /// <typeparam name="TState">The type of the state.</typeparam>
        /// <typeparam name="TResult">
        ///     The result type of the <see cref="Task{T}" /> returned by <paramref name="operation" />.
        /// </typeparam>
        /// <returns>
        ///     A task that will run to completion if the original task completes successfully (either the
        ///     first time or after retrying transient failures). If the task fails with a non-transient error or
        ///     the retry limit is reached, the returned task will become faulted and the exception must be observed.
        /// </returns>
        public static Task<TResult> ExecuteAsync<TState, TResult>(
            [NotNull] this IExecutionStrategy strategy,
            [NotNull] Func<TState, Task<TResult>> operation,
            [CanBeNull] TState state)
        {
            Check.NotNull(operation, nameof(operation));

            return strategy.ExecuteAsync((t, ct) => t.operation(t.state), new { operation, state }, default(CancellationToken));
        }

        /// <summary>
        ///     Executes the specified operation and returns the result.
        /// </summary>
        /// <param name="strategy">The strategy that will be used for the execution.</param>
        /// <param name="operation">
        ///     A delegate representing an executable operation that returns the result of type <typeparamref name="TResult" />.
        /// </param>
        /// <param name="state">The state that will be passed to the operation.</param>
        /// <typeparam name="TState">The type of the state.</typeparam>
        /// <typeparam name="TResult">The return type of <paramref name="operation" />.</typeparam>
        /// <returns>The result from the operation.</returns>
        public static TResult Execute<TState, TResult>(
            [NotNull] this IExecutionStrategy strategy,
            [NotNull] Func<TState, TResult> operation,
            [CanBeNull] TState state)
            => Check.NotNull(strategy, nameof(strategy)).Execute(operation, verifySucceeded: null, state: state);

        /// <summary>
        ///     Executes the specified asynchronous operation and returns the result.
        /// </summary>
        /// <param name="strategy">The strategy that will be used for the execution.</param>
        /// <param name="operation">
        ///     A function that returns a started task of type <typeparamref name="TResult" />.
        /// </param>
        /// <param name="cancellationToken">
        ///     A cancellation token used to cancel the retry operation, but not operations that are already in flight
        ///     or that already completed successfully.
        /// </param>
        /// <param name="state">The state that will be passed to the operation.</param>
        /// <typeparam name="TState">The type of the state.</typeparam>
        /// <typeparam name="TResult">
        ///     The result type of the <see cref="Task{T}" /> returned by <paramref name="operation" />.
        /// </typeparam>
        /// <returns>
        ///     A task that will run to completion if the original task completes successfully (either the
        ///     first time or after retrying transient failures). If the task fails with a non-transient error or
        ///     the retry limit is reached, the returned task will become faulted and the exception must be observed.
        /// </returns>
        public static Task<TResult> ExecuteAsync<TState, TResult>(
            [NotNull] this IExecutionStrategy strategy,
            [NotNull] Func<TState, CancellationToken, Task<TResult>> operation,
            [CanBeNull] TState state,
            CancellationToken cancellationToken)
            => Check.NotNull(strategy, nameof(strategy)).ExecuteAsync(operation, verifySucceeded: null, state: state, cancellationToken: cancellationToken);

        /// <summary>
        ///     Executes the specified operation in a transaction.
        /// </summary>
        /// <param name="strategy"> The strategy that will be used for the execution. </param>
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
            [NotNull] Action<TContext> operation,
            [NotNull] Func<TContext, bool> verifySucceeded,
            [NotNull] TContext context)
            where TContext : DbContext
            => strategy.ExecuteInTransaction<TContext, object>((c, s) => operation(c), (c, s) => verifySucceeded(c), null, context);

        /// <summary>
        ///     Executes the specified asynchronous operation in a transaction.
        /// </summary>
        /// <param name="strategy"> The strategy that will be used for the execution. </param>
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
            [NotNull] Func<TContext, Task> operation,
            [NotNull] Func<TContext, Task<bool>> verifySucceeded,
            [NotNull] TContext context)
            where TContext : DbContext
            => strategy.ExecuteInTransactionAsync<TContext, object>((c, s, ct) => operation(c), (c, s, ct) => verifySucceeded(c), null, context, default(CancellationToken));

        /// <summary>
        ///     Executes the specified asynchronous operation in a transaction.
        /// </summary>
        /// <param name="strategy"> The strategy that will be used for the execution. </param>
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
            [NotNull] Func<TContext, CancellationToken, Task> operation,
            [NotNull] Func<TContext, CancellationToken, Task<bool>> verifySucceeded,
            [NotNull] TContext context,
            CancellationToken cancellationToken = default(CancellationToken))
            where TContext : DbContext
            => strategy.ExecuteInTransactionAsync<TContext, object>((c, s, ct) => operation(c, ct), (c, s, ct) => verifySucceeded(c, ct), null, context, cancellationToken);

        /// <summary>
        ///     Executes the specified operation in a transaction and returns the result.
        /// </summary>
        /// <param name="strategy"> The strategy that will be used for the execution. </param>
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
            [NotNull] Func<TContext, TResult> operation,
            [NotNull] Func<TContext, bool> verifySucceeded,
            [NotNull] TContext context)
            where TContext : DbContext
            => strategy.ExecuteInTransaction<TContext, object, TResult>((c, s) => operation(c), (c, s) => verifySucceeded(c), null, context);

        /// <summary>
        ///     Executes the specified asynchronous operation in a transaction and returns the result.
        /// </summary>
        /// <param name="strategy"> The strategy that will be used for the execution. </param>
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
            [NotNull] Func<TContext, CancellationToken, Task<TResult>> operation,
            [NotNull] Func<TContext, CancellationToken, Task<bool>> verifySucceeded,
            [NotNull] TContext context,
            CancellationToken cancellationToken = default(CancellationToken))
            where TContext : DbContext
            => strategy.ExecuteInTransactionAsync<TContext, object, TResult>((c, s, ct) => operation(c, ct), (c, s, ct) => verifySucceeded(c, ct), null, context, cancellationToken);

        /// <summary>
        ///     Executes the specified operation in a transaction.
        /// </summary>
        /// <param name="strategy"> The strategy that will be used for the execution. </param>
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
            [NotNull] Action<TContext, TState> operation,
            [NotNull] Func<TContext, TState, bool> verifySucceeded,
            [CanBeNull] TState state,
            [NotNull] TContext context)
            where TContext : DbContext
            => strategy.ExecuteInTransaction((c, s) =>
                {
                    operation(c, s);
                    return true;
                },
                verifySucceeded, state, context);

        /// <summary>
        ///     Executes the specified asynchronous operation in a transaction.
        /// </summary>
        /// <param name="strategy"> The strategy that will be used for the execution. </param>
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
            [NotNull] Func<TContext, TState, CancellationToken, Task> operation,
            [NotNull] Func<TContext, TState, CancellationToken, Task<bool>> verifySucceeded,
            [CanBeNull] TState state,
            [NotNull] TContext context,
            CancellationToken cancellationToken = default(CancellationToken))
            where TContext : DbContext
            => strategy.ExecuteInTransactionAsync(async (c, s, ct) =>
                {
                    await operation(c, s, ct);
                    return true;
                }, verifySucceeded, state, context, cancellationToken);

        /// <summary>
        ///     Executes the specified operation in a transaction and returns the result.
        /// </summary>
        /// <param name="strategy"> The strategy that will be used for the execution. </param>
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
            [NotNull] Func<TContext, TState, TResult> operation,
            [NotNull] Func<TContext, TState, bool> verifySucceeded,
            [CanBeNull] TState state,
            [NotNull] TContext context)
            where TContext : DbContext
            => ExecuteInTransaction(strategy, operation, verifySucceeded, state, context, c => c.Database.BeginTransaction());

        /// <summary>
        ///     Executes the specified asynchronous operation in a transaction and returns the result.
        /// </summary>
        /// <param name="strategy"> The strategy that will be used for the execution. </param>
        /// <param name="operation">
        ///     A function that returns a started task of type <typeparamref name="TResult" />.
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
        /// <typeparam name="TResult"> The result type of the <see cref="Task{T}" /> returned by <paramref name="operation" />. </typeparam>
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
            [NotNull] Func<TContext, TState, CancellationToken, Task<TResult>> operation,
            [NotNull] Func<TContext, TState, CancellationToken, Task<bool>> verifySucceeded,
            [CanBeNull] TState state,
            [NotNull] TContext context,
            CancellationToken cancellationToken = default(CancellationToken))
            where TContext : DbContext
            => ExecuteInTransactionAsync(strategy, operation, verifySucceeded, state, context, (c, ct) => c.Database.BeginTransactionAsync(ct), cancellationToken);

        /// <summary>
        ///     Executes the specified operation in a transaction and returns the result.
        /// </summary>
        /// <param name="strategy"> The strategy that will be used for the execution. </param>
        /// <param name="operation">
        ///     A delegate representing an executable operation that returns the result of type <typeparamref name="TResult" />.
        /// </param>
        /// <param name="verifySucceeded">
        ///     A delegate that tests whether the operation succeeded even though an exception was thrown when the
        ///     transaction was being committed.
        /// </param>
        /// <param name="state"> The state that will be passed to the operation. </param>
        /// <param name="context"> The context that will be used to start the transaction. </param>
        /// <param name="beginTransaction"> A delegate that begins a transaction using the given context. </param>
        /// <typeparam name="TContext"> The type of the supplied context instance. </typeparam>
        /// <typeparam name="TState"> The type of the state. </typeparam>
        /// <typeparam name="TResult"> The return type of <paramref name="operation" />. </typeparam>
        /// <returns> The result from the operation. </returns>
        /// <exception cref="RetryLimitExceededException">
        ///     Thrown if the operation has not succeeded after the configured number of retries.
        /// </exception>
        public static TResult ExecuteInTransaction<TContext, TState, TResult>(
            [NotNull] IExecutionStrategy strategy,
            [NotNull] Func<TContext, TState, TResult> operation,
            [NotNull] Func<TContext, TState, bool> verifySucceeded,
            [CanBeNull] TState state,
            [NotNull] TContext context,
            [NotNull] Func<TContext, IDbContextTransaction> beginTransaction)
            where TContext : DbContext
            => Check.NotNull(strategy, nameof(strategy)).Execute(s =>
                {
                    Check.NotNull(beginTransaction, nameof(beginTransaction));
                    using (var transaction = beginTransaction(s.Context))
                    {
                        s.CommitFailed = false;
                        s.Result = s.Operation(s.Context, s.State);
                        s.CommitFailed = true;
                        transaction.Commit();
                    }
                    return s.Result;
                },
                s => new ExecutionResult<TResult>(s.CommitFailed && s.VerifySucceeded(s.Context, s.State), s.Result),
                new ExecutionState<TContext, TState, TResult>(
                    Check.NotNull(operation, nameof(operation)), Check.NotNull(verifySucceeded, nameof(verifySucceeded)), state, Check.NotNull(context, nameof(context))));

        /// <summary>
        ///     Executes the specified asynchronous operation in a transaction and returns the result.
        /// </summary>
        /// <param name="strategy"> The strategy that will be used for the execution. </param>
        /// <param name="operation">
        ///     A function that returns a started task of type <typeparamref name="TResult" />.
        /// </param>
        /// <param name="verifySucceeded">
        ///     A delegate that tests whether the operation succeeded even though an exception was thrown when the
        ///     transaction was being committed.
        /// </param>
        /// <param name="state"> The state that will be passed to the operation. </param>
        /// <param name="context"> The context that will be used to start the transaction. </param>
        /// <param name="beginTransaction"> A delegate that begins a transaction using the given context. </param>
        /// <param name="cancellationToken">
        ///     A cancellation token used to cancel the retry operation, but not operations that are already in flight
        ///     or that already completed successfully.
        /// </param>
        /// <typeparam name="TContext"> The type of the supplied context instance. </typeparam>
        /// <typeparam name="TState"> The type of the state. </typeparam>
        /// <typeparam name="TResult"> The result type of the <see cref="Task{T}" /> returned by <paramref name="operation" />. </typeparam>
        /// <returns>
        ///     A task that will run to completion if the original task completes successfully (either the
        ///     first time or after retrying transient failures). If the task fails with a non-transient error or
        ///     the retry limit is reached, the returned task will become faulted and the exception must be observed.
        /// </returns>
        /// <exception cref="RetryLimitExceededException">
        ///     Thrown if the operation has not succeeded after the configured number of retries.
        /// </exception>
        public static Task<TResult> ExecuteInTransactionAsync<TContext, TState, TResult>(
            [NotNull] IExecutionStrategy strategy,
            [NotNull] Func<TContext, TState, CancellationToken, Task<TResult>> operation,
            [NotNull] Func<TContext, TState, CancellationToken, Task<bool>> verifySucceeded,
            [CanBeNull] TState state,
            [NotNull] TContext context,
            [NotNull] Func<DbContext, CancellationToken, Task<IDbContextTransaction>> beginTransaction,
            CancellationToken cancellationToken = default(CancellationToken))
            where TContext : DbContext
            => Check.NotNull(strategy, nameof(strategy)).ExecuteAsync(async (s, ct) =>
                {
                    Check.NotNull(beginTransaction, nameof(beginTransaction));
                    using (var transaction = await beginTransaction(s.Context, cancellationToken))
                    {
                        s.CommitFailed = false;
                        s.Result = await s.Operation(s.Context, s.State, ct);
                        s.CommitFailed = true;
                        transaction.Commit();
                    }
                    return s.Result;
                },
                async (s, c) => new ExecutionResult<TResult>(s.CommitFailed && await s.VerifySucceeded(s.Context, s.State, c), s.Result),
                new ExecutionStateAsync<TContext, TState, TResult>(
                    Check.NotNull(operation, nameof(operation)), Check.NotNull(verifySucceeded, nameof(verifySucceeded)), state, Check.NotNull(context, nameof(context))));

        private class ExecutionState<TContext, TState, TResult>
            where TContext : DbContext
        {
            public ExecutionState(
                Func<TContext, TState, TResult> operation,
                Func<TContext, TState, bool> verifySucceeded,
                TState state,
                TContext context)
            {
                Operation = operation;
                VerifySucceeded = verifySucceeded;
                State = state;
                Context = context;
            }

            public Func<TContext, TState, TResult> Operation { get; }
            public Func<TContext, TState, bool> VerifySucceeded { get; }
            public TState State { get; }
            public TContext Context { get; }
            public TResult Result { get; set; }
            public bool CommitFailed { get; set; }
        }

        private class ExecutionStateAsync<TContext, TState, TResult>
            where TContext : DbContext
        {
            public ExecutionStateAsync(
                Func<TContext, TState, CancellationToken, Task<TResult>> operation,
                Func<TContext, TState, CancellationToken, Task<bool>> verifySucceeded,
                TState state,
                TContext context)
            {
                Operation = operation;
                VerifySucceeded = verifySucceeded;
                State = state;
                Context = context;
            }

            public Func<TContext, TState, CancellationToken, Task<TResult>> Operation { get; }
            public Func<TContext, TState, CancellationToken, Task<bool>> VerifySucceeded { get; }
            public TState State { get; }
            public TContext Context { get; }
            public TResult Result { get; set; }
            public bool CommitFailed { get; set; }
        }
    }
}
