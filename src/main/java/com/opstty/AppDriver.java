package com.opstty;

import com.opstty.job.*;
import com.opstty.mapper.OldestTreeMapper;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("districts", Districts.class,
                    "A map/reduce program that counts the districts.");
            programDriver.addClass("species", Species.class,
                    "A map/reduce program that counts the species.");
            programDriver.addClass("heightSort", HeightSort.class,
                    "A map/reduce program that sort the trees by height from smallest to largest.");
            programDriver.addClass("maxHeight", MaxHeight.class,
                    "A map/reduce program that calculate the maximum height per kind of tree.");
            programDriver.addClass("treeCount", TreeCount.class,
                    "A map/reduce program that counts the number of trees by kind.");
            programDriver.addClass("oldestTree", OldestTree.class,
                    "A map/reduce program that display the district where the oldest tree is.");
            programDriver.addClass("maxTreeDistrict", MaxTreeDistrict.class,
                    "A map/reduce program that district that contains the most trees.");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
