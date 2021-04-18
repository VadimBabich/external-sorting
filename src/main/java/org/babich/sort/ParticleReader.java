package org.babich.sort;

import java.io.*;
import java.util.Iterator;

/**
 * The reader of a batch of sorted data
 *
 * @author Vadim Babich
 */
public interface ParticleReader extends Closeable, Iterator<Integer> {

    boolean hasNext();

    @Override
    default Integer next() {
        return readInt();
    }

    int readInt();

    class FileBackedParticleReader implements ParticleReader {

        private final DataInputStream dataInputStream;
        int size;
        int remaining;

        private FileBackedParticleReader(InputStream dataInputStream, int size) {
            this.size = size;
            this.remaining = size;
            this.dataInputStream = new DataInputStream(dataInputStream);
        }

        @Override
        public boolean hasNext() {
            return 0 != this.remaining;
        }

        @Override
        public int readInt() {
            try {
                int value = dataInputStream.readInt();
                this.remaining--;
                return value;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void close() throws IOException {
            try {
                dataInputStream.close();
            } catch (Exception ignore) {
            }
        }

        static class FileBackedParticleReaderBuilder {
            private File file;
            private int size = -1;
            private InputStream inputStream;

            public FileBackedParticleReaderBuilder withFile(File file) {
                this.file = file;
                return this;
            }

            public FileBackedParticleReaderBuilder withInputStream(InputStream inputStream) {
                this.inputStream = inputStream;
                return this;
            }

            public FileBackedParticleReaderBuilder withSize(int size) {
                this.size = size;
                return this;
            }

            private FileBackedParticleReader create(File file, int size){
                try {
                    return new FileBackedParticleReader(new BufferedInputStream(new FileInputStream(file)), size);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            public FileBackedParticleReader build() {
                if(-1 == size){
                    throw new IllegalArgumentException("Size must be initialized.");
                }
                if (null != file) {
                    return create(file, size);
                }

                if (null != inputStream) {
                    return new FileBackedParticleReader(inputStream, size);
                }

                throw new IllegalStateException("Reader cannot be created without an input stream.");
            }

        }
    }

}
