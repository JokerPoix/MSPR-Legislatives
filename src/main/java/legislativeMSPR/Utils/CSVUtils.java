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
        List<String[]> rows = new ArrayList<>();
        try (var br = new java.io.BufferedReader(new java.io.FileReader(csvFilePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                rows.add(line.split(";", -1)); // -1 pour garder les vides
            }
        } catch (IOException e) {
            System.err.println("Erreur lors de la lecture du fichier CSV : " + e.getMessage());
            return;
        }

        if (rows.isEmpty()) {
            System.out.println("[CSV vide]");
            return;
        }

        // Calcul de la largeur max pour chaque colonne
        int colCount = rows.get(0).length;
        int[] colWidths = new int[colCount];
        for (String[] row : rows) {
            for (int i = 0; i < colCount; i++) {
                if (i < row.length)
                    colWidths[i] = Math.max(colWidths[i], row[i].length());
            }
        }

        // Fonction pour afficher une ligne joliment
        java.util.function.Consumer<String[]> printRow = row -> {
            StringBuilder sb = new StringBuilder("| ");
            for (int i = 0; i < colCount; i++) {
                String cell = i < row.length ? row[i] : "";
                sb.append(String.format("%-" + colWidths[i] + "s", cell)).append(" | ");
            }
            System.out.println(sb.toString());
        };

        // Affichage des parties
        int total = rows.size();
        int head = Math.min(6, total);
        int tail = Math.min(5, total - head);
        int available = total - head - tail;
        int middle = Math.min(2, Math.max(available, 0));

        System.out.println("=== Aperçu du CSV ===\n");

        System.out.println("-- 6 premières lignes --");
        for (int i = 0; i < head; i++) printRow.accept(rows.get(i));

        if (tail > 0) {
            System.out.println("\n-- 5 dernières lignes --");
            for (int i = total - tail; i < total; i++) printRow.accept(rows.get(i));
        }

        if (middle > 0) {
            System.out.println("\n-- Lignes aléatoires au milieu --");
            Random rand = new Random();
            Set<Integer> indices = new HashSet<>();
            while (indices.size() < middle) {
                indices.add(rand.nextInt(available) + head);
            }
            for (int idx : indices) printRow.accept(rows.get(idx));
        }
    }

    
    public static void previewXlsx(String xlsxFilePath) {
        List<String[]> rows = new ArrayList<>();
        try (var fis = new java.io.FileInputStream(new File(xlsxFilePath));
             Workbook workbook = WorkbookFactory.create(fis)) {

            Sheet sheet = workbook.getSheetAt(0);
            for (org.apache.poi.ss.usermodel.Row row : sheet) {
                List<String> cellValues = new ArrayList<>();
                for (Cell cell : row) {
                    cellValues.add(cell.toString());
                }
                rows.add(cellValues.toArray(new String[0]));
            }

        } catch (IOException e) {
            System.err.println("Erreur lors de la lecture du fichier XLSX : " + e.getMessage());
            return;
        }

        if (rows.isEmpty()) {
            System.out.println("[XLSX vide]");
            return;
        }

        int colCount = rows.get(0).length;
        int[] colWidths = new int[colCount];
        for (String[] row : rows) {
            for (int i = 0; i < colCount; i++) {
                if (i < row.length)
                    colWidths[i] = Math.max(colWidths[i], row[i].length());
            }
        }

        java.util.function.Consumer<String[]> printRow = row -> {
            StringBuilder sb = new StringBuilder("| ");
            for (int i = 0; i < colCount; i++) {
                String cell = i < row.length ? row[i] : "";
                sb.append(String.format("%-" + colWidths[i] + "s", cell)).append(" | ");
            }
            System.out.println(sb.toString());
        };

        int total = rows.size();
        int head = Math.min(6, total);
        int tail = Math.min(5, total - head);
        int available = total - head - tail;
        int middle = Math.min(2, Math.max(available, 0));

        System.out.println("=== Aperçu du XLSX ===");

        System.out.println("\n-- 6 premières lignes --");
        for (int i = 0; i < head; i++) printRow.accept(rows.get(i));

        if (tail > 0) {
            System.out.println("\n-- 5 dernières lignes --");
            for (int i = total - tail; i < total; i++) printRow.accept(rows.get(i));
        }

        if (middle > 0) {
            System.out.println("\n-- Lignes aléatoires au milieu --");
            Random rand = new Random();
            Set<Integer> indices = new HashSet<>();
            while (indices.size() < middle) {
                indices.add(rand.nextInt(available) + head);
            }
            for (int idx : indices) printRow.accept(rows.get(idx));
        }
    }

}
