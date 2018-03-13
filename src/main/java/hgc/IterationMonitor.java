package hgc;

import java.util.function.BiConsumer;

@FunctionalInterface
public interface IterationMonitor<T> extends BiConsumer<Integer, HGC<T>> {
    @Override
    void accept(Integer iteration, HGC<T> HGC);
}
