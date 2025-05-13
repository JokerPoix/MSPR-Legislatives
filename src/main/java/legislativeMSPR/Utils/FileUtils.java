package legislativeMSPR.Utils;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class FileUtils {

    /**
     * Retourne tous les fichiers *.csv et *.xlsx :
     *  - soit directement sous baseDir
     *  - soit, s’il n’y en a pas, dans chacun de ses sous-répertoires
     */
    public static List<File> listInputFiles(File baseDir) {
        List<File> result = new ArrayList<>();
        if (baseDir == null || !baseDir.exists()) return result;

        // 1) fichiers directement dans baseDir ?
        File[] direct = baseDir.listFiles((d, name) ->
            name.toLowerCase().endsWith(".csv") ||
            name.toLowerCase().endsWith(".xlsx")
        );
        if (direct != null && direct.length > 0) {
            result.addAll(Arrays.asList(direct));
            return result;
        }

        // 2) sinon, on parcourt chaque sous-répertoire
        File[] subdirs = baseDir.listFiles(File::isDirectory);
        if (subdirs != null) {
            for (File y : subdirs) {
                File[] files = y.listFiles((d, name) ->
                    name.toLowerCase().endsWith(".csv") ||
                    name.toLowerCase().endsWith(".xlsx")
                );
                if (files != null) {
                    result.addAll(Arrays.asList(files));
                }
            }
        }
        return result;
    }

    /**
     * Retourne tous les fichiers (sans filtrage d'extension) 
     * sous le répertoire donné.
     */
    public static List<File> list(String dir) {
        File d = new File(dir);
        if (!d.exists()) return List.of();
        return Arrays.stream(d.listFiles())
                     .filter(File::isFile)
                     .collect(Collectors.toList());
    }
}
