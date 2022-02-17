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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.exchange.ExchangeSource;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class FileSystemExchangeSource
        implements ExchangeSource
{
    @GuardedBy("this")
    private final Iterator<ExchangeSourceFile> exchangeSourceFileIterator;
    @GuardedBy("this")
    private final List<ExchangeStorageReader> readers;
    @GuardedBy("this")
    private boolean closed;

    public FileSystemExchangeSource(
            FileSystemExchangeStorage exchangeStorage,
            List<ExchangeSourceFile> exchangeSourceFiles,
            int maxPageStorageSize,
            int exchangeSourceConcurrentReaders)
    {
        requireNonNull(exchangeStorage, "exchangeStorage is null");
        this.exchangeSourceFileIterator =
                ImmutableList.copyOf(requireNonNull(exchangeSourceFiles, "exchangeSourceFiles is null")).iterator();

        int numReaders = min(exchangeSourceFiles.size(), exchangeSourceConcurrentReaders);
        this.readers = new ArrayList<>(numReaders);
        for (int i = 0; i < numReaders; ++i) {
            try {
                readers.add(exchangeStorage.createExchangeStorageReader(exchangeSourceFileIterator, maxPageStorageSize));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public synchronized CompletableFuture<Void> isBlocked()
    {
        if (readers.isEmpty()) {
            return NOT_BLOCKED;
        }
        return toCompletableFuture(whenAnyComplete(
                readers.stream()
                        .map(ExchangeStorageReader::isBlocked)
                        .collect(toImmutableList())));
    }

    @Override
    public synchronized boolean isFinished()
    {
        return closed || readers.isEmpty();
    }

    @Nullable
    @Override
    public synchronized Slice read()
    {
        if (isFinished()) {
            return null;
        }

        Iterator<ExchangeStorageReader> iterator = readers.iterator();

        while (iterator.hasNext()) {
            ExchangeStorageReader reader = iterator.next();

            if (reader.isBlocked().isDone()) {
                Slice slice;
                try {
                    slice = reader.read();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                if (slice == null) {
                    verify(!exchangeSourceFileIterator.hasNext());
                    iterator.remove();
                }
                else {
                    return slice;
                }
            }
        }

        return null;
    }

    @Override
    public synchronized long getMemoryUsage()
    {
        return readers.stream().mapToLong(ExchangeStorageReader::getRetainedSize).sum();
    }

    @Override
    public synchronized void close()
    {
        if (!closed) {
            closed = true;
            readers.forEach(ExchangeStorageReader::close);
        }
    }
}
