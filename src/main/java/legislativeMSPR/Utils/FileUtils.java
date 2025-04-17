package legislativeMSPR.Utils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class FileUtils {

    /**
     * Retourne tous les fichiers *.csv et *.xlsx sous baseDir/<année>/*
     */
    public static List<File> listInputFiles(File baseDir) {
        List<File> result = new ArrayList<>();
        File[] years = baseDir.listFiles(File::isDirectory);
        if (years == null) return result;
        for (File y : years) {
            for (File f : y.listFiles((d, name) ->
                name.toLowerCase().endsWith(".csv") ||
                name.toLowerCase().endsWith(".xlsx"))) {
                result.add(f);
            }
        }
        return result;
    }
}
