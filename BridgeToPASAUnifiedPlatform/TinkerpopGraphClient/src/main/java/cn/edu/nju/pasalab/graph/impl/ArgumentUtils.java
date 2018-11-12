package cn.edu.nju.pasalab.graph.impl;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ArgumentUtils {
    // To a list seperated by comma
    public static List<String> toList(String arguments) {
        if (arguments == null) {
            return null;
        } else {
            return Arrays.stream(arguments.split(",")).collect(Collectors.toList());
        }
    }
}
