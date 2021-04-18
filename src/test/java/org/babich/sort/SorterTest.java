package org.babich.sort;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.Collector;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 *
 * @author Vadim Babich
 */
class SorterTest {

    @Test
    void givenRandomItems_WhenThemSorting_ThenExpectSortedResult() {

        int size = 91;
        int[] source = new int[size];
        Random random = new Random();

        IntStream.range(0, size).forEach(idx -> source[idx] = random.nextInt());

        Collector<Integer, Sorter, Merger> sortedParticleCollector = Sorter.getSortedParticleCollector(30);

        int[] result = new int[size];

        IntStream.of(source)
                .boxed()
                .collect(sortedParticleCollector)
                .doMergeIn(new Consumer<Integer>() {
                    int idx = 0;

                    @Override
                    public void accept(Integer integer) {
                        result[idx++] = integer;
                    }
                });

        Arrays.sort(source);
        Assertions.assertArrayEquals(source, result);
    }

}