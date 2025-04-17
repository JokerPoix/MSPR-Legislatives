package legislativeMSPR;

import legislativeMSPR.Utils.CSVUtils;
import legislativeMSPR.Utils.DataIngestionUtils;
import legislativeMSPR.Utils.FileUtils;
import legislativeMSPR.DataCleaner;
import legislativeMSPR.DataAggregator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        // Réduire les logs
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        // Initialisation de Spark
        SparkSession spark = SparkSession.builder()
                .appName("rendementLoc$")
                .config("spark.master", "local[*]")
                .config("spark.driver.memory", "16g")
                .config("spark.executor.memory", "8g")
                .config("spark.executor.cores", "4")
                .config("spark.sql.shuffle.partitions", "16")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
     // ==== Insertion en base via MySqlWriter ====
        MySqlWriter writer = new MySqlWriter(
        	    "localhost", 
        	    "3306", 
        	    "MSPRLegislative", 
        	    "poix", 
        	    "poix"
        	);
        writer.dropAllTables();
        // --- Traitement des résultats électoraux ---
        String inputBase = "src/main/resources/inputs/FinalResultsDatasLeg";
        String outputBase = "src/main/resources/outputs/FinalResultsDatasLeg";
        File baseDir = new File(inputBase);

        for (File inFile : FileUtils.listInputFiles(baseDir)) {
        	String fileName = inFile.getName();
            // si c'est le fichier de référence, on skip
            if (fileName.startsWith("Reference")) {
                continue;
            }
        	String year = inFile.getParentFile().getName();
            Dataset<Row> raw = DataIngestionUtils.loadAsDataset(spark, inFile);

            // Filtrage géographique
            Dataset<Row> cleaned = DataCleaner.filterByBretagne(raw);

            // Agrégation des scores Gauche/Droite
            String refPath = inputBase + "/" + year + "/ReferenceDroiteGauche.csv";
            Dataset<Row> aggregated = DataAggregator.aggregateScores(spark, cleaned, refPath);

            // Export CSV
            String outDir = outputBase + "/" + year;
            new File(outDir).mkdirs();
            String outFile = inFile.getName().replaceAll("\\.xlsx?$", ".csv");
            CSVUtils.saveAsSingleCSV(aggregated, outDir, outFile);
            CSVUtils.previewCsv(outDir + "/" + outFile);

            // Construction du nom de table MySQL sans point ni extension
            String baseName       = inFile.getName().replaceFirst("(?i)\\.xlsx?$", "");
            String[] tokens       = baseName.split("[ _-]");
            String localisation   = tokens[tokens.length - 1];
            // Ne conserver que lettres, chiffres et underscore
            String safeLoc        = localisation.replaceAll("[^A-Za-z0-9]", "_");
            String tableName      = String.format("ResultatLegislative_%s_%s", year, safeLoc);

            // Debug : vérifier le nom final
            System.out.println("→ j’écris dans la table MySQL : " + tableName);

            writer.writeTable(aggregated, tableName);
        }

        // --- Traitement du fichier de sécurité ---
        String secInput = "src/main/resources/inputs/Security/CriminalityFranceDepartement.xlsx";
        File secFile = new File(secInput);
        if (secFile.exists()) {
            Dataset<Row> secRaw = DataIngestionUtils.loadAsDataset(spark, secFile);
            Dataset<Row> secGeo = DataCleaner.filterByBretagne(secRaw);
            Dataset<Row> secFiltered = DataCleaner.filterByYears(secGeo);
            Dataset<Row> secPivot = DataAggregator.pivotByYear(secFiltered);
            String secOutDir = "src/main/resources/outputs/Security";
            new File(secOutDir).mkdirs();
            String secCsv = "CriminaliteFranceDepartement.csv";
            CSVUtils.saveAsSingleCSV(secPivot, secOutDir, secCsv);
            CSVUtils.previewCsv(secOutDir + "/" + secCsv);
            writer.writeTable(secPivot, "CriminaliteFranceDep");
        } else {
            System.out.println("[Main] Fichier de sécurité introuvable : " + secInput);
        }
        // Fichiers prets à mettre en BDD
        String readyDir = "src/main/resources/inputs/ReadyForDataBase";
        File readyBase = new File(readyDir);
        if (readyBase.exists()) {
            for (File inFile : FileUtils.listInputFiles(readyBase)) {
                // on récupère le nom de base sans extension
                String baseName = inFile.getName().replaceFirst("(?i)\\.xlsx?$", "")
                                          .replaceFirst("(?i)\\.csv$", "");
                // charger en DataFrame
                Dataset<Row> df = DataIngestionUtils.loadAsDataset(spark, inFile);
                // créer un nom de table safe (lettres, chiffres, _)
                String table = baseName.replaceAll("[^A-Za-z0-9]", "_");
                System.out.println("→ j’écris en BDD le fichier “" + inFile.getName()
                                   + "” dans la table : " + table);
                writer.writeTable(df, table);
            }
        } else {
            System.out.println("[Main] Aucun fichier prêt pour la BDD dans " + readyDir);
        }
        
     // Dump de la base MySQL
        try {
            String dumpFile = "src/main/resources/outputs/MSPRLegislative.dump";
            ProcessBuilder pb = new ProcessBuilder(
                "mysqldump", "-u", "poix", "-ppoix", "MSPRLegislative"
            );
            pb.redirectOutput(new File(dumpFile));
            Process p = pb.start();
            int exitCode = p.waitFor();
            System.out.println("→ Dump MySQL terminé (code " + exitCode + "), fichier : " + dumpFile);
        } catch (IOException | InterruptedException e) {
            System.err.println("Erreur durant le dump MySQL : " + e.getMessage());
        }
        spark.stop();
    }
}
