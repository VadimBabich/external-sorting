package org.babich.sort;

import org.babich.sort.ParticleReader.FileBackedParticleReader.FileBackedParticleReaderBuilder;

import java.io.*;
import java.nio.file.Files;

/**
 * the write a batch of sorted data to a temporary file.
 * {@code size} - number of elements in a batch
 *
 * @author Vadim Babich
 */
public interface ParticleWriter {

    void writeInt(int value);

    ParticleReader asParticleReader();

    class FileBackedParticleWriter implements ParticleWriter, Closeable {

        private final DataOutputStream dataOutputStream;
        private final File file;
        private int size;

        private FileBackedParticleWriter(OutputStream outputStream, File file) {
            this.size = 0;
            this.file = file;
            this.dataOutputStream = new DataOutputStream(outputStream);
        }

        @Override
        public void writeInt(int value) {
            try {
                dataOutputStream.writeInt(value);
                size++;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public ParticleReader asParticleReader() {
            close();
            return new FileBackedParticleReaderBuilder()
                    .withSize(size)
                    .withFile(file)
                    .build();
        }

        @Override
        public void close() {
            try {
                dataOutputStream.close();
            } catch (Exception ignore) {
            }
        }

        public static FileBackedParticleStorageBuilder newParticleWriter(){
            return new FileBackedParticleStorageBuilder();
        }

        public static class FileBackedParticleStorageBuilder {

            private File file;
            private OutputStream outputStream;

            public FileBackedParticleStorageBuilder withFile(File file) {
                this.file = file;
                return this;
            }

            public FileBackedParticleStorageBuilder withOutputStream(OutputStream outputStream) {
                this.outputStream = outputStream;
                return this;
            }

            private FileBackedParticleWriter create(File file){
                try {
                    return new FileBackedParticleWriter(new BufferedOutputStream(new FileOutputStream(file)), file);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            public FileBackedParticleWriter build() {
                if (null != file) {
                    return create(file);
                }

                if (null != outputStream) {
                    return new FileBackedParticleWriter(outputStream, file);
                }

                try {
                    return create(Files.createTempFile("sorter", "").toFile());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

        }
    }
}
