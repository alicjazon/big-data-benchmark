package utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class SearchFiles {

    public static ArrayList<String> search(String path, String searchString) throws IOException {
        File searchDir = new File(path);
        ArrayList<String> matches = checkFiles(searchDir.listFiles(), searchString, new ArrayList<String>());
        return matches;
    }

    private static ArrayList<String> checkFiles(File[] files, String search, ArrayList<String> acc) throws IOException {
        for (File file : files) {
            if (file.isDirectory()) {
                checkFiles(file.listFiles(), search, acc);
            } else {
                String result = fileContainsString(file, search);
                if (result != null) {
                    acc.add(result);
                }
            }
        }
        return acc;
    }

    private static String fileContainsString(File file, String search) throws IOException {
        BufferedReader in = new BufferedReader(new FileReader(file));
        String line;
        while ((line = in.readLine()) != null) {
            if (line.contains(search)) {
                in.close();
                return line;
            }
        }
        in.close();
        return null;
    }
}