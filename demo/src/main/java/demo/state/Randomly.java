package demo.state;

import java.util.Random;

public final class Randomly {

    // @Override
    // public static <T> T fromOptions(T[] options) {
    //     return options[new Random().nextInt(options.length)];
    // }

    @SafeVarargs
    public static <T> T fromOptions(T... options) {
        return options[new Random().nextInt(options.length)];
    }

    // TODO: generate table columns creation query etc.
}
