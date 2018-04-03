package ihdn;

import java.util.function.BiFunction;

@FunctionalInterface
public interface IterationMonitor extends BiFunction<Integer, IHDN, Boolean> {
    @Override
    Boolean apply(Integer iteration, IHDN IHDN);
}
