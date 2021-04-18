package org.babich.sort;

import com.google.common.io.Closer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Merging sorted data from batches into the result consumer.
 *
 * @author Vadim Babich
 */
public interface Merger {

    void doMergeIn(Consumer<Integer> consumer);


    class MultiWayMerger implements Merger {

        private final List<ParticleReader> particles;

        public MultiWayMerger(List<ParticleReader> particleReaders) {
            this.particles = new ArrayList<>(particleReaders);
        }

        @Override
        public void doMergeIn(Consumer<Integer> consumer) {
            //noinspection UnstableApiUsage
            try (Closer closer = particles.stream().collect(Closer::create, Closer::register, Closer::register)) {
                merge(initialBuffer(), consumer);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        int[] initialBuffer() {
            return particles.stream().mapToInt(ParticleReader::readInt).toArray();
        }

        void merge(int[] bufferedParticleValues, Consumer<Integer> resultConsumer) {
            int minValueIndex;
            for (; bufferedParticleValues.length > 0
                    ; bufferedParticleValues = readParticle(bufferedParticleValues, minValueIndex)) {

                minValueIndex = findMinValueIndexOf(bufferedParticleValues);
                resultConsumer.accept(bufferedParticleValues[minValueIndex]);
            }
        }

        int[] readParticle(int[] bufferedParticleValues, int minValueIndex) {
            ParticleReader reader = particles.get(minValueIndex);
            if (reader.hasNext()) {
                bufferedParticleValues[minValueIndex] = reader.readInt();
                return bufferedParticleValues;
            }

            return removeEmptyReader(bufferedParticleValues, minValueIndex);
        }

        int[] removeEmptyReader(int[] bufferedParticleValues, int minValueIndex) {
            particles.remove(minValueIndex);
            return removeItemByIndex(bufferedParticleValues, minValueIndex);
        }

        int findMinValueIndexOf(int[] particlesMinValues) {
            int maxValue = Integer.MAX_VALUE;
            int minValuePos = -1;
            int pos = 0;
            for (; pos < particlesMinValues.length; pos++) {
                if (maxValue < particlesMinValues[pos]) {
                    continue;
                }
                maxValue = particlesMinValues[pos];
                minValuePos = pos;
            }
            return minValuePos;
        }


        int[] removeItemByIndex(int[] array, int index) {
            int[] buff = new int[array.length - 1];
            for (int i = 0, j = 0; i < array.length; i++) {
                if (i == index) {
                    continue;
                }
                buff[j++] = array[i];
            }
            return buff;
        }

    }

}
