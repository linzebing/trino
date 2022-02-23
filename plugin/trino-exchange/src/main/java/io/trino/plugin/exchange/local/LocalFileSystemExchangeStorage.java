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
package io.trino.plugin.exchange.local;

import com.google.common.collect.ImmutableList;
import com.google.common.io.MoreFiles;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.plugin.exchange.ExchangeSourceFile;
import io.trino.plugin.exchange.ExchangeStorageReader;
import io.trino.plugin.exchange.ExchangeStorageWriter;
import io.trino.plugin.exchange.FileStatus;
import io.trino.plugin.exchange.FileSystemExchangeStorage;
import io.trino.spi.TrinoException;
import org.openjdk.jol.info.ClassLayout;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.Math.toIntExact;
import static java.nio.file.Files.createFile;
import static java.util.Objects.requireNonNull;

public class LocalFileSystemExchangeStorage
        implements FileSystemExchangeStorage
{
    private static final int BUFFER_SIZE_IN_BYTES = toIntExact(DataSize.of(4, KILOBYTE).toBytes());

    @Override
    public void createDirectories(URI dir)
            throws IOException
    {
        Files.createDirectories(Paths.get(dir.getPath()));
    }

    @Override
    public ExchangeStorageReader createExchangeStorageReader(Iterator<ExchangeSourceFile> exchangeSourceFiles, int maxPageStorageSize)
            throws IOException
    {
        return new LocalExchangeStorageReader(exchangeSourceFiles);
    }

    @Override
    public ExchangeStorageWriter createExchangeStorageWriter(URI file, Optional<SecretKey> secretKey)
            throws IOException
    {
        return new LocalExchangeStorageWriter(file, secretKey);
    }

    @Override
    public boolean exists(URI file)
    {
        return Files.exists(Paths.get(file.getPath()));
    }

    @Override
    public ListenableFuture<Void> createEmptyFile(URI file)
    {
        try {
            createFile(Paths.get(file.getPath()));
        }
        catch (IOException | RuntimeException e) {
            return immediateFailedFuture(e);
        }
        return immediateVoidFuture();
    }

    @Override
    public ListenableFuture<Void> deleteRecursively(URI dir)
    {
        try {
            MoreFiles.deleteRecursively(Paths.get(dir.getPath()), ALLOW_INSECURE);
        }
        catch (IOException | RuntimeException e) {
            return immediateFailedFuture(e);
        }
        return immediateVoidFuture();
    }

    @Override
    public List<FileStatus> listFiles(URI dir)
            throws IOException
    {
        ImmutableList.Builder<FileStatus> builder = ImmutableList.builder();
        for (Path file : listPaths(dir, Files::isRegularFile)) {
            builder.add(new FileStatus(file.toUri().toString(), Files.size(file)));
        }
        return builder.build();
    }

    @Override
    public List<URI> listDirectories(URI dir)
            throws IOException
    {
        ImmutableList.Builder<URI> builder = ImmutableList.builder();
        for (Path subDir : listPaths(dir, Files::isDirectory)) {
            builder.add(subDir.toUri());
        }
        return builder.build();
    }

    @Override
    public int getWriteBufferSize()
    {
        return BUFFER_SIZE_IN_BYTES;
    }

    @Override
    public void close()
    {
    }

    private static List<Path> listPaths(URI directory, Predicate<Path> predicate)
            throws IOException
    {
        ImmutableList.Builder<Path> builder = ImmutableList.builder();
        try (Stream<Path> dir = Files.list(Paths.get(directory.getPath()))) {
            dir.filter(predicate).forEach(builder::add);
        }
        return builder.build();
    }

    private static class LocalExchangeStorageReader
            implements ExchangeStorageReader
    {
        private final Iterator<ExchangeSourceFile> exchangeSourceFiles;
        private InputStreamSliceInput sliceInput;

        public LocalExchangeStorageReader(Iterator<ExchangeSourceFile> exchangeSourceFiles)
                throws FileNotFoundException
        {
            this.exchangeSourceFiles = requireNonNull(exchangeSourceFiles, "exchangeSourceFiles is null");
            checkArgument(exchangeSourceFiles.hasNext(), "no exchangeSourceFile to read from");

            this.sliceInput = getSliceInput(exchangeSourceFiles.next());
        }

        @Override
        public Slice read()
                throws IOException
        {
            while (sliceInput.isReadable() || exchangeSourceFiles.hasNext()) {
                if (sliceInput.isReadable()) {
                    return sliceInput.readSlice(sliceInput.readInt());
                }
                else {
                    sliceInput = getSliceInput(exchangeSourceFiles.next());
                }
            }
            return null;
        }

        @Override
        public ListenableFuture<Void> isBlocked()
        {
            return immediateVoidFuture();
        }

        @Override
        public long getRetainedSize()
        {
            return sliceInput.getRetainedSize();
        }

        @Override
        public void close()
        {
            sliceInput.close();
        }

        private InputStreamSliceInput getSliceInput(ExchangeSourceFile exchangeSourceFile)
                throws FileNotFoundException
        {
            File file = Paths.get(exchangeSourceFile.getFileUri()).toFile();
            Optional<SecretKey> secretKey = exchangeSourceFile.getSecretKey();
            if (secretKey.isPresent()) {
                try {
                    Cipher cipher = Cipher.getInstance("AES");
                    cipher.init(Cipher.DECRYPT_MODE, secretKey.get());
                    return new InputStreamSliceInput(new CipherInputStream(new FileInputStream(file), cipher), BUFFER_SIZE_IN_BYTES);
                }
                catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException e) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to create CipherInputStream: " + e.getMessage(), e);
                }
            }
            else {
                return new InputStreamSliceInput(new FileInputStream(file), BUFFER_SIZE_IN_BYTES);
            }
        }
    }

    private static class LocalExchangeStorageWriter
            implements ExchangeStorageWriter
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(LocalExchangeStorageWriter.class).instanceSize();

        private final OutputStream outputStream;

        public LocalExchangeStorageWriter(URI file, Optional<SecretKey> secretKey)
                throws FileNotFoundException
        {
            if (secretKey.isPresent()) {
                try {
                    Cipher cipher = Cipher.getInstance("AES");
                    cipher.init(Cipher.ENCRYPT_MODE, secretKey.get());
                    this.outputStream = new CipherOutputStream(new FileOutputStream(Paths.get(file.getPath()).toFile()), cipher);
                }
                catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException e) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to create CipherOutputStream: " + e.getMessage(), e);
                }
            }
            else {
                this.outputStream = new FileOutputStream(Paths.get(file.getPath()).toFile());
            }
        }

        @Override
        public ListenableFuture<Void> write(Slice slice)
        {
            try {
                outputStream.write(slice.getBytes());
            }
            catch (IOException | RuntimeException e) {
                return immediateFailedFuture(e);
            }
            return immediateVoidFuture();
        }

        @Override
        public ListenableFuture<Void> finish()
        {
            try {
                outputStream.close();
            }
            catch (IOException | RuntimeException e) {
                return immediateFailedFuture(e);
            }
            return immediateVoidFuture();
        }

        @Override
        public ListenableFuture<Void> abort()
        {
            try {
                outputStream.close();
            }
            catch (IOException | RuntimeException e) {
                return immediateFailedFuture(e);
            }
            return immediateVoidFuture();
        }

        @Override
        public long getRetainedSize()
        {
            return INSTANCE_SIZE;
        }
    }
}
