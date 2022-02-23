/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.exchange;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.exchange.ExchangeSink;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.crypto.SecretKey;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

@ThreadSafe
public class FileSystemExchangeSink
        implements ExchangeSink
{
    public static final String COMMITTED_MARKER_FILE_NAME = "committed";
    public static final String DATA_FILE_SUFFIX = ".data";

    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FileSystemExchangeSink.class).instanceSize();

    private final FileSystemExchangeStorage exchangeStorage;
    private final URI outputDirectory;
    private final int outputPartitionCount;
    private final int maxPageStorageSize;
    private final Optional<SecretKey> secretKey;
    private final BufferPool bufferPool;

    private final Map<Integer, BufferedStorageWriter> writersMap = new ConcurrentHashMap<>();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private volatile boolean closed;

    public FileSystemExchangeSink(
            FileSystemExchangeStorage exchangeStorage,
            URI outputDirectory,
            int outputPartitionCount,
            Optional<SecretKey> secretKey,
            int maxPageStorageSize,
            int exchangeSinkBufferPoolMinSize)
    {
        this.exchangeStorage = requireNonNull(exchangeStorage, "exchangeStorage is null");
        this.outputDirectory = requireNonNull(outputDirectory, "outputDirectory is null");
        this.outputPartitionCount = outputPartitionCount;
        this.maxPageStorageSize = maxPageStorageSize;
        this.secretKey = requireNonNull(secretKey, "secretKey is null");
        // double buffering to overlap computation and I/O
        this.bufferPool = new BufferPool(max(outputPartitionCount * 2, exchangeSinkBufferPoolMinSize), exchangeStorage.getWriteBufferSize());
    }

    // The future returned by {@link #isBlocked()} should only be considered as a best-effort hint.
    @Override
    public CompletableFuture<Void> isBlocked()
    {
        return bufferPool.isBlocked();
    }

    @Override
    public void add(int partitionId, Slice data)
    {
        throwIfFailed();

        checkArgument(partitionId < outputPartitionCount, "partition id is expected to be less than %s: %s", outputPartitionCount, partitionId);

        int requiredPageStorageSize = Integer.BYTES + data.length();
        if (requiredPageStorageSize > maxPageStorageSize) {
            throw new TrinoException(NOT_SUPPORTED, format("Page required storage size (%s) larger than max allowed size (%s)", requiredPageStorageSize, maxPageStorageSize));
        }

        // Ensure no new writers can be created after `closed` is set to true
        BufferedStorageWriter writer;
        synchronized (this) {
            if (closed) {
                return;
            }
            writer = writersMap.computeIfAbsent(partitionId, this::createWriter);
        }
        writer.write(data);
    }

    private BufferedStorageWriter createWriter(int partitionId)
    {
        URI outputPath = outputDirectory.resolve(partitionId + DATA_FILE_SUFFIX);
        try {
            return new BufferedStorageWriter(exchangeStorage.createExchangeStorageWriter(outputPath, secretKey), bufferPool, failure);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long getMemoryUsage()
    {
        return INSTANCE_SIZE
                + bufferPool.getRetainedSize()
                + estimatedSizeOf(writersMap, SizeOf::sizeOf, BufferedStorageWriter::getRetainedSize);
    }

    @Override
    public synchronized CompletableFuture<Void> finish()
    {
        if (closed) {
            return failedFuture(new IllegalStateException("Exchange sink has already closed"));
        }

        ListenableFuture<Void> finishFuture = asVoid(Futures.allAsList(
                writersMap.values().stream().map(BufferedStorageWriter::finish).collect(toImmutableList())));
        addSuccessCallback(finishFuture, this::destroy);
        finishFuture = Futures.transformAsync(
                finishFuture,
                ignored -> exchangeStorage.createEmptyFile(outputDirectory.resolve(COMMITTED_MARKER_FILE_NAME)),
                directExecutor());
        Futures.addCallback(finishFuture, new FutureCallback<>()
        {
            @Override
            public void onSuccess(Void result)
            {
                closed = true;
            }

            @Override
            public void onFailure(Throwable ignored)
            {
                abort();
            }
        }, directExecutor());

        return toCompletableFuture(finishFuture);
    }

    @Override
    public synchronized CompletableFuture<Void> abort()
    {
        if (closed) {
            return completedFuture(null);
        }
        closed = true;

        ListenableFuture<Void> abortFuture = asVoid(Futures.allAsList(
                writersMap.values().stream().map(BufferedStorageWriter::abort).collect(toImmutableList())));
        addSuccessCallback(abortFuture, this::destroy);

        return toCompletableFuture(Futures.transformAsync(
                abortFuture,
                ignored -> exchangeStorage.deleteRecursively(outputDirectory),
                directExecutor()));
    }

    private void throwIfFailed()
    {
        Throwable throwable = failure.get();
        if (throwable != null) {
            throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    private void destroy()
    {
        writersMap.clear();
        bufferPool.close();
    }

    @ThreadSafe
    private static class BufferedStorageWriter
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(BufferedStorageWriter.class).instanceSize();

        private final ExchangeStorageWriter storageWriter;
        private final BufferPool bufferPool;
        private final AtomicReference<Throwable> failure;

        @GuardedBy("this")
        private SliceOutput currentBuffer;

        public BufferedStorageWriter(ExchangeStorageWriter storageWriter, BufferPool bufferPool, AtomicReference<Throwable> failure)
        {
            this.storageWriter = requireNonNull(storageWriter, "storageWriter is null");
            this.bufferPool = requireNonNull(bufferPool, "bufferPool is null");
            this.failure = requireNonNull(failure, "failure is null");
        }

        public synchronized void write(Slice data)
        {
            writeInternal(Slices.wrappedIntArray(data.length()));
            writeInternal(data);
        }

        public synchronized ListenableFuture<Void> finish()
        {
            flushIfNeeded(true);
            return storageWriter.finish();
        }

        public synchronized ListenableFuture<Void> abort()
        {
            return storageWriter.abort();
        }

        public synchronized long getRetainedSize()
        {
            return INSTANCE_SIZE + storageWriter.getRetainedSize();
        }

        private void writeInternal(Slice slice)
        {
            int position = 0;
            while (position < slice.length()) {
                if (currentBuffer == null) {
                    currentBuffer = bufferPool.take();
                    if (currentBuffer == null) {
                        // buffer pool is closed
                        return;
                    }
                }
                int writableBytes = min(currentBuffer.writableBytes(), slice.length() - position);
                currentBuffer.writeBytes(slice.getBytes(position, writableBytes));
                position += writableBytes;

                flushIfNeeded(false);
            }
        }

        private void flushIfNeeded(boolean finished)
        {
            SliceOutput buffer = currentBuffer;
            if (!buffer.isWritable() || finished) {
                if (!buffer.isWritable()) {
                    currentBuffer = null;
                }
                ListenableFuture<Void> writeFuture = storageWriter.write(buffer.slice());
                writeFuture.addListener(() -> bufferPool.offer(buffer), directExecutor());
                addExceptionCallback(writeFuture, throwable -> failure.compareAndSet(null, throwable));
            }
        }
    }

    @ThreadSafe
    private static class BufferPool
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(BufferPool.class).instanceSize();

        private final int numBuffers;
        private final long bufferRetainedSize;
        @GuardedBy("this")
        private final Queue<SliceOutput> freeBuffersQueue;
        @GuardedBy("this")
        private CompletableFuture<Void> blockedFuture = new CompletableFuture<>();
        @GuardedBy("this")
        private boolean closed;

        public BufferPool(int numBuffers, int writeBufferSize)
        {
            checkArgument(numBuffers >= 1, "numBuffers must be at least one");

            this.numBuffers = numBuffers;
            this.freeBuffersQueue = new ArrayDeque<>(numBuffers);
            for (int i = 0; i < numBuffers; ++i) {
                freeBuffersQueue.add(Slices.allocate(writeBufferSize).getOutput());
            }
            this.bufferRetainedSize = freeBuffersQueue.peek().getRetainedSize();
        }

        public synchronized CompletableFuture<Void> isBlocked()
        {
            if (freeBuffersQueue.isEmpty()) {
                if (blockedFuture.isDone()) {
                    blockedFuture = new CompletableFuture<>();
                }
                return blockedFuture;
            }
            else {
                return NOT_BLOCKED;
            }
        }

        public synchronized SliceOutput take()
        {
            while (true) {
                if (closed) {
                    return null;
                }
                if (!freeBuffersQueue.isEmpty()) {
                    return freeBuffersQueue.poll();
                }
                try {
                    wait();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }

        public void offer(SliceOutput buffer)
        {
            buffer.reset();

            CompletableFuture<Void> completableFuture;
            synchronized (this) {
                if (closed) {
                    return;
                }
                completableFuture = blockedFuture;
                freeBuffersQueue.add(buffer);
                notify();
            }

            completableFuture.complete(null);
        }

        public synchronized long getRetainedSize()
        {
            if (closed) {
                return INSTANCE_SIZE;
            }
            else {
                return INSTANCE_SIZE + numBuffers * bufferRetainedSize;
            }
        }

        public void close()
        {
            CompletableFuture<Void> completableFuture;
            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
                notifyAll();
                completableFuture = blockedFuture;
                freeBuffersQueue.clear();
            }

            completableFuture.complete(null);
        }
    }
}
