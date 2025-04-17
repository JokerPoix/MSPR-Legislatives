package legislativeMSPR.Utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class CSVUtils {

    public static void saveAsSingleCSV(Dataset<Row> dataset, String outputFolder, String fileName) {
        String tempPath = outputFolder + "/temp_output";
        dataset.coalesce(1)
               .write()
               .option("header", "true")
               .option("delimiter", ";")
               .mode("overwrite")
               .csv(tempPath);

        File tempDir = new File(tempPath);
        File finalDir = new File(outputFolder);
        File[] files = tempDir.listFiles((dir, name) -> name.startsWith("part-"));
        if (files != null && files.length > 0) {
            files[0].renameTo(new File(finalDir, fileName));
        }
        for (File f : Objects.requireNonNull(tempDir.listFiles())) {
            f.delete();
        }
        tempDir.delete();
    }

    public static void previewCsv(String csvFilePath) {
        // Lire le fichier CSV ligne par ligne sans dépendance java.nio
        List<String> lines = new ArrayList<>();
        try (java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader(csvFilePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                lines.add(line);
            }
        } catch (java.io.IOException e) {
            System.err.println("Erreur lors de la lecture du fichier CSV : " + e.getMessage());
            return;
        }

        int total       = lines.size();
        int headCount   = Math.min(6, total);
        int tailCount   = Math.min(5, total - headCount);
        int available   = total - headCount - tailCount;
        int randomCount = Math.min(2, Math.max(available, 0));

        System.out.println("=== Aperçu du CSV ===");

        System.out.println("\n-- 6 premières lignes --");
        for (int i = 0; i < headCount; i++) {
            System.out.println(lines.get(i));
        }

        System.out.println("\n-- 5 dernières lignes --");
        for (int i = total - tailCount; i < total; i++) {
            System.out.println(lines.get(i));
        }

        if (randomCount > 0) {
            System.out.println("\n-- " + randomCount + " lignes aléatoires au milieu --");
            Random rand = new Random();
            Set<Integer> indices = new HashSet<>();
            while (indices.size() < randomCount) {
                indices.add(rand.nextInt(available) + headCount);
            }
            for (int idx : indices) {
                System.out.println(lines.get(idx));
            }
        }
    }
    
    public static void previewXlsx(String xlsxFilePath) {
        List<String> rows = new ArrayList<>();
        try (var fis = new java.io.FileInputStream(new File(xlsxFilePath));
             Workbook workbook = WorkbookFactory.create(fis)) {

            Sheet sheet = workbook.getSheetAt(0);
            for (org.apache.poi.ss.usermodel.Row row : sheet) {
                StringBuilder sb = new StringBuilder();
                for (Cell cell : row) {
                    sb.append(cell.toString()).append(";");
                }
                if (sb.length() > 0) sb.setLength(sb.length() - 1);
                rows.add(sb.toString());
            }

            int total       = rows.size();
            int headCount   = Math.min(6, total);
            int tailCount   = Math.min(5, total - headCount);
            int available   = total - headCount - tailCount;
            int randomCount = Math.min(2, Math.max(available, 0));

            System.out.println("=== Aperçu du XLSX ===");

            System.out.println("\n-- 6 premières lignes --");
            for (int i = 0; i < headCount; i++) {
                System.out.println(rows.get(i));
            }

            System.out.println("\n-- 5 dernières lignes --");
            for (int i = total - tailCount; i < total; i++) {
                System.out.println(rows.get(i));
            }

            if (randomCount > 0) {
                System.out.println("\n-- " + randomCount + " lignes aléatoires au milieu --");
                Random rand = new Random();
                Set<Integer> indices = new HashSet<>();
                while (indices.size() < randomCount) {
                    indices.add(rand.nextInt(available) + headCount);
                }
                for (int idx : indices) {
                    System.out.println(rows.get(idx));
                }
            }

        } catch (IOException e) {
            System.err.println("Erreur lors de la lecture du fichier XLSX : " + e.getMessage());
        }
    }
}
