package org.babich.sort;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Non sorted stream split to k - sorted batches and persist them in external memory (hard disk).
 * Then it reads in k sorted batches by chunks and merging them into a single sorted list.
 *
 * @author Vadim Babich
 */
public class Sorter implements Consumer<Integer> {

    private final int batchSize;
    private int batchNumber;
    private int batchCount;

    private final Map<Integer, ParticleWriter> particles = new HashMap<>();

    private int[] batch;

    Sorter(int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than zero.");
        }

        this.batchSize = batchSize;
        this.batch = new int[batchSize];

        this.batchNumber = 0;
        this.batchCount = 0;
    }

    @Override
    public void accept(Integer value) {
        if (batchCount == batchSize) {
            sort(batch);
        }
        batch[batchCount++] = value;
    }

    public Merger finish() {
        sort(batch);
        return createMerger();
    }

    private Merger createMerger() {
        return new Merger.MultiWayMerger(particles.values().stream()
                .map(ParticleWriter::asParticleReader)
                .collect(Collectors.toList()));
    }

    private void sort(int[] batch) {
        if (batchCount != batchSize) {
            batch = Arrays.copyOfRange(batch, 0, batchCount);
        }
        Arrays.sort(batch);

        ParticleWriter particleWriter = particles.computeIfAbsent(batchNumber++, this::createParticleWriter);

        IntStream.of(batch).forEach(particleWriter::writeInt);

        this.batch = new int[batchSize];
        batchCount = 0;
    }

    protected ParticleWriter createParticleWriter(int key){
        return ParticleWriter.FileBackedParticleWriter.newParticleWriter().build();
    }

    /**
     * Using as collector for the non sorted data stream.
     * @param particleSize - the maximum size of a batch of items that can be sorted in memory.
     * @return
     */
    public static Collector<Integer, Sorter, Merger> getSortedParticleCollector(int particleSize) {
        return Collector.of(() -> new Sorter(particleSize)
                , Sorter::accept
                , (l, r) -> l
                , Sorter::finish
                , Collector.Characteristics.UNORDERED);
    }

}
