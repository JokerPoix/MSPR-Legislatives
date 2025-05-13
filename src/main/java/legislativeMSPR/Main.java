package legislativeMSPR;

import legislativeMSPR.Utils.CSVUtils;
import legislativeMSPR.Utils.DataIngestionUtils;
import legislativeMSPR.Utils.FileUtils;
import legislativeMSPR.Utils.MySQLConverter;
import legislativeMSPR.config.SparkContextProvider;
import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        var spark = SparkContextProvider.create();
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
        String inputDatasLegBase = "src/main/resources/inputs/FinalResultsDatasLeg";
        String outputDatasLegBase = "src/main/resources/outputs/FinalResultsDatasLeg";
        File baseDir = new File(inputDatasLegBase);

        for (File inFile : FileUtils.listInputFiles(baseDir)) {
        	String fileName = inFile.getName();
            // si c'est le fichier de référence, on skip
            if (fileName.startsWith("Reference")) {
                continue;
            }
        	String year = inFile.getParentFile().getName();
            Dataset<Row> raw = DataIngestionUtils.loadAsDataset(spark, inFile);

            // Filtrage géographique
            Dataset<Row> cleaned = DataAggregator.ensureGeoColumns(raw);
            cleaned =DataCleaner.filterByBretagne(cleaned);

            // Agrégation des scores Gauche/Droite
            String refPath = inputDatasLegBase + "/" + year + "/ReferenceDroiteGauche.csv";
            Dataset<Row> aggregated = DataAggregator.aggregateScores(spark, cleaned, refPath);
            
            // Export CSV
            String outDir = outputDatasLegBase + "/" + year;
            new File(outDir).mkdirs();
            String outFile = inFile.getName().replaceAll("\\.xlsx?$", ".csv");
            CSVUtils.saveAsSingleCSV(aggregated, outDir, outFile);
            CSVUtils.previewCsv(outDir + "/" + outFile);

        }

     // --- Traitement des loyers par commune (CarteLoyer) ---
        String baseCarte   = "src/main/resources/inputs/CarteLoyer";
        String outputCarte = "src/main/resources/outputs/CarteLoyer";

        // 0) création du dossier de base
        File baseOut = new File(outputCarte);
        if (!baseOut.exists() && !baseOut.mkdirs()) {
            throw new RuntimeException("Impossible de créer le dossier " + outputCarte);
        }

        // 1) on recherche dynamiquement tous les sous-dossiers (années) existants
        File baseCarteDir = new File(baseCarte);
        if (!baseCarteDir.exists()) {
            System.err.println("[Main] Dossier d'entrée introuvable : " + baseCarte);
        } else {
            for (File yearDir : baseCarteDir.listFiles(File::isDirectory)) {
                String year = yearDir.getName();
                System.out.println("[Main] Traitement CarteLoyer pour l'année " + year);

                // 2) création du sous-dossier de sortie pour cette année
                File outYearDir = new File(baseOut, year);
                if (!outYearDir.exists() && !outYearDir.mkdirs()) {
                    System.err.println("! Impossible de créer le dossier " + outYearDir.getPath());
                    continue;
                }

                // 3) itération sur tous les fichiers Excel/xlsx du dossier année
                List<File> files = FileUtils.listInputFiles(yearDir);
                if (files.isEmpty()) {
                    System.out.println("[Main] Aucun fichier de loyers trouvé pour " + year);
                    continue;
                }

                for (File loFile : files) {
                    String fileName = loFile.getName();
                    System.out.println("→ Traitement fichier : " + loFile.getPath());

                    // 4) Lecture et nettoyage
                    Dataset<Row> loRaw   = DataIngestionUtils.loadAsDataset(spark, loFile);
                    Dataset<Row> loGeo   = DataAggregator.ensureGeoColumns(loRaw);
                    Dataset<Row> loClean = DataCleaner.filterByBretagne(loGeo);

                    // 5) (Éventuelles transformations spécifiques aux loyers…)

                    // 6) Export CSV
                    String outCsv = fileName.replaceFirst("(?i)\\.xlsx?$", ".csv");
                    CSVUtils.saveAsSingleCSV(loClean, outYearDir.getPath(), outCsv);
                    CSVUtils.previewCsv(outYearDir.getPath() + "/" + outCsv);

            
                }
            }
        }


     // --- Traitement des données démographiques (niveau d'étude) ---
        String eduInput = "src/main/resources/inputs/Demographie-NiveauEtude/2022/population-selon-niveau-etude-par-commune.xlsx";
        File eduFile = new File(eduInput);
        if (eduFile.exists()) {
            // 1) lecture
            Dataset<Row> eduRaw = DataIngestionUtils.loadAsDataset(spark, eduFile);

            // 2)  on s'assure d'avoir Code département, Code de la commune et CODGEO
            Dataset<Row> eduGeo = DataAggregator.ensureGeoColumns(eduRaw);
            eduGeo = DataCleaner.filterByBretagne(eduGeo);
            // 3) agrégation des taux (universitaire vs bac ou moins, par tranche d'âge)
            Dataset<Row> eduAgg = DataAggregator.aggregateEducation(eduGeo);

            // 4) export CSV
            String eduOutDir = "src/main/resources/outputs/Demographie-NiveauEtude/2022";
            new File(eduOutDir).mkdirs();
            String eduCsv = "population-niveau-etude-par-commune.csv";
            CSVUtils.saveAsSingleCSV(eduAgg, eduOutDir, eduCsv);
            CSVUtils.previewCsv(eduOutDir + "/" + eduCsv);

        } else {
            System.out.println("[Main] Fichier démographie niveau d'étude introuvable : " + eduInput);
        }

        // --- Traitement du fichier de sécurité ---
        String secInput = "src/main/resources/inputs/Security/criminalite-par-departement.xlsx";
        File secFile = new File(secInput);
        if (secFile.exists()) {
            Dataset<Row> secRaw = DataIngestionUtils.loadAsDataset(spark, secFile);
            Dataset<Row> secGeo = DataCleaner.filterByBretagne(secRaw);
            Dataset<Row> secFiltered = DataCleaner.filterByYears(secGeo);
            Dataset<Row> secPivot = DataAggregator.pivotByYear(secFiltered);
            secPivot = DataAggregator.ensureGeoColumns(secPivot);
            String secOutDir = "src/main/resources/outputs/Security";
            new File(secOutDir).mkdirs();
            String secCsv = "criminalite-par-departement.csv";
            CSVUtils.saveAsSingleCSV(secPivot, secOutDir, secCsv);
            CSVUtils.previewCsv(secOutDir + "/" + secCsv);
            writer.writeTable(secPivot, "CriminaliteFranceDep");
        } else {
            System.out.println("[Main] Fichier de sécurité introuvable : " + secInput);
        }
        // Fichiers prets à mettre en BDD
        String readyIn  = "src/main/resources/inputs/ReadyForDataBase";
        String readyOut = "src/main/resources/outputs/ReadyForDataBaseCsv";
        File basereadyBDDIn  = new File(readyIn);
        File basereadyBDDOut = new File(readyOut);
        if (!basereadyBDDOut.exists() && !basereadyBDDOut.mkdirs()) {
            throw new RuntimeException("Impossible de créer le dossier de sortie " + readyOut);
        }

        if (basereadyBDDIn.exists()) {
            for (File inFile : FileUtils.listInputFiles(basereadyBDDIn)) {
                String relPath = basereadyBDDIn.toPath().relativize(inFile.toPath()).toString();
                if (!relPath.toLowerCase().matches(".*\\.xlsx?$")) continue;

                String outRelative = relPath.replaceFirst("(?i)\\.xlsx?$", ".csv");
                File outFile = new File(basereadyBDDOut, outRelative);
                outFile.getParentFile().mkdirs();

                System.out.println("→ Conversion : " + inFile.getPath() + " → " + outFile.getPath());
                Dataset<Row> df = DataIngestionUtils.loadAsDataset(spark, inFile);
                df = DataAggregator.ensureGeoColumns(df);
                df = df.withColumn(
                	    "Code département",
                	    col("Code département").cast("int")
                	);
                CSVUtils.saveAsSingleCSV(df, outFile.getParent(), outFile.getName());
            }
        } else {
            System.out.println("[Main] Aucun dossier ReadyForDataBase trouvé en " + readyIn);
        }
        
        //Gestion des communes niveau BDD
        File geoFile = new File("src/main/resources/inputs/FichierReferenceCommunesDepartement/communes-france.csv");
        MySQLConverter.loadAndWriteCommunesDepartement(spark, writer, geoFile);
        MySQLConverter.processOutputCSVs(
                spark,
                writer,
                "src/main/resources/outputs"
            );
        MySQLConverter.processDepartementsCSVsOnCommunes(spark, writer, "src/main/resources/outputs");

        //  Dump de la base MySQL

        try {
            String dumpFile = "MSPRLegislative.dump";
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
