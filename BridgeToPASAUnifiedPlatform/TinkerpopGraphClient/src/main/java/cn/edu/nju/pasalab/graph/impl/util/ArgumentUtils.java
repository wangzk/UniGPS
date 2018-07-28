package cn.edu.nju.pasalab.graph.impl.util;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ArgumentUtils {
    // To a list seperated by comma
    public static List<String> toList(String arguments) {
        return Arrays.stream(arguments.split(",")).collect(Collectors.toList());
    }
}
