package cn.edu.nju.pasalab.graph.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ArgumentUtils {
    // To a list seperated by comma
    public static List<String> toList(String arguments) {
        if (arguments != null) {
            return Arrays.stream(arguments.split(",")).collect(Collectors.toList());
        } else {
            return new ArrayList<>();
        }

    }
}
