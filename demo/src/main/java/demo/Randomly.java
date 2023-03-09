package demo;

import java.util.Random;

/**
 * Partially copied from SQLancer
 */
public final class Randomly {
    private static final ThreadLocal<Random> THREAD_RANDOM = new ThreadLocal<>();
    private long seed;

    public Randomly() {
        THREAD_RANDOM.set(new Random());
    }

    public Randomly(long seed) {
        this.seed = seed;
        THREAD_RANDOM.set(new Random(seed));
    }

    @SafeVarargs
    public static <T> T fromOptions(T... options) {
        return options[getNextInt(0, options.length)];
    }

    // see https://stackoverflow.com/a/2546158
    // uniformity does not seem to be important for us
    // SQLancer previously used ThreadLocalRandom.current().nextLong(lower, upper)
    private static long getNextLong(long lower, long upper) {
        if (lower > upper) {
            throw new IllegalArgumentException(lower + " " + upper);
        }
        if (lower == upper) {
            return lower;
        }
        return getThreadRandom().get().longs(lower, upper).findFirst().getAsLong();
    }

    private static int getNextInt(int lower, int upper) {
        return (int) getNextLong(lower, upper);
    }

    public static int smallNumber() {
        // no need to cache for small numbers
        return (int) (Math.abs(getThreadRandom().get().nextGaussian())) * 2;
    }

    private static ThreadLocal<Random> getThreadRandom() {
        if (THREAD_RANDOM.get() == null) {
            // a static method has been called, before Randomly was instantiated
            THREAD_RANDOM.set(new Random());
        }
        return THREAD_RANDOM;
    }

    public static long getNotCachedInteger(int lower, int upper) {
        return getNextLong(lower, upper);
    }

    public static boolean getBooleanWithRatherLowProbability() {
        return getThreadRandom().get().nextInt(10) == 1;
    }

    private static boolean smallBiasProbability() {
        return getThreadRandom().get().nextInt(100) == 1;
    }

    public static boolean getBooleanWithSmallProbability() {
        return smallBiasProbability();
    }

    
}
