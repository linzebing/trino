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

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.spi.exchange.ExchangeSink;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.concurrent.GuardedBy;
import javax.crypto.SecretKey;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class FileSystemExchangeSink
        implements ExchangeSink
{
    public static final String COMMITTED_MARKER_FILE_NAME = "committed";
    public static final String DATA_FILE_SUFFIX = ".data";

    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FileSystemExchangeSink.class).instanceSize();
    private static final int INTEGER_INSTANCE_SIZE = ClassLayout.parseClass(Integer.class).instanceSize();

    private final FileSystemExchangeStorage exchangeStorage;
    private final URI outputDirectory;
    private final int outputPartitionCount;
    private final int numBuffers;
    private final Optional<SecretKey> secretKey;
    private final int writeBufferSize;

    private final Map<Integer, ExchangeStorageWriter> writers = new ConcurrentHashMap<>();
    private final Map<Integer, SliceOutput> pendingBuffers = new ConcurrentHashMap<>();
    private final LinkedBlockingQueue<SliceOutput> freeBuffers = new LinkedBlockingQueue<>();
    private volatile boolean finished;
    @GuardedBy("this")
    private CompletableFuture<Void> blockedFuture = new CompletableFuture<>();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    public FileSystemExchangeSink(FileSystemExchangeStorage exchangeStorage, URI outputDirectory, int outputPartitionCount, Optional<SecretKey> secretKey)
    {
        this.exchangeStorage = requireNonNull(exchangeStorage, "exchangeStorage is null");
        this.outputDirectory = requireNonNull(outputDirectory, "outputDirectory is null");
        this.outputPartitionCount = outputPartitionCount;
        this.numBuffers = outputPartitionCount * 2; // double buffering to overlap computation and I/O
        this.secretKey = requireNonNull(secretKey, "secretKey is null");
        this.writeBufferSize = exchangeStorage.getWriteBufferSizeInBytes();

        for (int i = 0; i < numBuffers; ++i) {
            freeBuffers.add(Slices.allocate(writeBufferSize).getOutput());
        }
    }

    // This return result is more like a best-effort hint. The implementation is not thread-safe,
    // but doesn't affect correctness. It's possible to return NOT_BLOCKED when it's actually
    // blocked in certain cases. The decision is consciously made to avoid global locking.
    @Override
    public CompletableFuture<?> isBlocked()
    {
        if (freeBuffers.isEmpty() && pendingBuffers.size() < outputPartitionCount) {
            synchronized (this) {
                if (blockedFuture.isDone()) {
                    blockedFuture = new CompletableFuture<>();
                }
                return blockedFuture;
            }
        }
        else {
            return NOT_BLOCKED;
        }
    }

    @Override
    public void add(int partitionId, Slice data)
    {
        throwIfFailed();

        checkArgument(partitionId < outputPartitionCount, "partition id is expected to be less than %s: %s", outputPartitionCount, partitionId);
        checkState(!finished, "already finished");
        if (finished) {
            return;
        }

        writers.computeIfAbsent(partitionId, this::createWriter);
        synchronized (writers.get(partitionId)) {
            writeToExchangeStorage(partitionId, Slices.wrappedIntArray(data.length()));
            writeToExchangeStorage(partitionId, data);
        }
    }

    private ExchangeStorageWriter createWriter(int partitionId)
    {
        URI outputPath = outputDirectory.resolve(partitionId + DATA_FILE_SUFFIX);
        try {
            return exchangeStorage.createExchangeStorageWriter(outputPath, secretKey);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void writeToExchangeStorage(int partitionId, Slice slice)
    {
        int position = 0;
        while (position < slice.length()) {
            SliceOutput pendingBuffer = pendingBuffers.computeIfAbsent(partitionId, ignored -> {
                try {
                    return freeBuffers.take();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });
            int writableBytes = min(pendingBuffer.writableBytes(), slice.length() - position);
            pendingBuffer.writeBytes(slice.getBytes(position, writableBytes));
            position += writableBytes;

            flushIfNeeded(partitionId, false);
        }
    }

    private void flushIfNeeded(int partitionId, boolean finished)
    {
        SliceOutput buffer = pendingBuffers.get(partitionId);
        if (!buffer.isWritable() || finished) {
            if (!buffer.isWritable()) {
                pendingBuffers.remove(partitionId);
            }
            writers.get(partitionId).write(buffer.slice()).whenComplete((result, throwable) -> {
                buffer.reset();
                freeBuffers.add(buffer);

                if (throwable != null) {
                    failure.compareAndSet(null, throwable);
                }
                else {
                    // Now we get released free buffer, unblock any previous blocked future
                    CompletableFuture<?> completableFuture;
                    synchronized (this) {
                        completableFuture = blockedFuture;
                    }
                    if (!completableFuture.isDone()) {
                        completableFuture.complete(null);
                    }
                }
            });
        }
    }

    @Override
    public long getMemoryUsage()
    {
        if (finished) {
            return INSTANCE_SIZE;
        }
        else {
            return INSTANCE_SIZE
                    + (long) writers.size() * INTEGER_INSTANCE_SIZE
                    + (long) pendingBuffers.size() * INTEGER_INSTANCE_SIZE
                    + (long) numBuffers * writeBufferSize;
        }
    }

    @Override
    public CompletableFuture<?> finish()
    {
        if (finished) {
            return completedFuture(null);
        }
        finished = true;
        for (Integer partitionId : writers.keySet()) {
            flushIfNeeded(partitionId, true);
        }
        return allOf(writers.values().stream().map(ExchangeStorageWriter::finish).toArray(CompletableFuture[]::new))
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        abort();
                    }
                })
                .thenRun(this::clear)
                .thenCompose(ignored -> exchangeStorage.createEmptyFile(outputDirectory.resolve(COMMITTED_MARKER_FILE_NAME)));
    }

    @Override
    public CompletableFuture<?> abort()
    {
        if (finished) {
            return completedFuture(null);
        }
        finished = true;
        return allOf(writers.values().stream().map(ExchangeStorageWriter::abort).toArray(CompletableFuture[]::new))
                .thenRun(this::clear)
                .thenCompose(ignored -> exchangeStorage.deleteRecursively(outputDirectory));
    }

    private void throwIfFailed()
    {
        Throwable throwable = failure.get();
        if (throwable != null) {
            throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    private void clear()
    {
        pendingBuffers.clear();
        freeBuffers.clear();
        writers.clear();
    }
}
