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
        YearlyProcessor.processByYear(
        	    spark,
        	    "src/main/resources/inputs/CarteLoyer",
        	    "src/main/resources/outputs/CarteLoyer"
        	);
     // --- Traitement de la production energetique par commune  ---
        YearlyProcessor.processByYear(
        	    spark,
        	    "src/main/resources/inputs/ProductionEnergetique",
        	    "src/main/resources/outputs/ProductionEnergetique"
        	);
     // --- Traitement des unites urbaines par commune  ---
        YearlyProcessor.processByYear(
        	    spark,
        	    "src/main/resources/inputs/UniteUrbaine",
        	    "src/main/resources/outputs/UniteUrbaine"
        	);

     // --- Traitement des données de pauvreté par commune  ---
        String povertyInput = "src/main/resources/inputs/Pauvrete/2017/niveau-pauvrete-par-commune.xlsx";
        File povertyFile = new File(povertyInput);
        if (povertyFile.exists()) {
            // 1) lecture
            Dataset<Row> povertyRaw = DataIngestionUtils.loadAsDataset(spark, povertyFile);

            // 2)  on s'assure d'avoir Code département, Code de la commune et CODGEO
            Dataset<Row> povertyGeo = DataAggregator.ensureGeoColumns(povertyRaw);
            povertyGeo = DataCleaner.filterByBretagne(povertyGeo);
            
            // 3) agrégation des taux de pauvreté
            Dataset<Row> povertyAgg = DataAggregator.averagePovertyByCommune(povertyGeo);

            // 4) export CSV
            String povertyOutDir = "src/main/resources/outputs/Pauvrete/2017";
            new File(povertyOutDir).mkdirs();
            String povertyCsv = "niveau-pauvrete-par-commune.csv";
            CSVUtils.saveAsSingleCSV(povertyAgg, povertyOutDir, povertyCsv);
            CSVUtils.previewCsv(povertyOutDir + "/" + povertyCsv);

        } else {
            System.out.println("[Main] Fichier pauvreté introuvable : " + povertyInput);
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
        String secInput = "src/main/resources/inputs/Security/criminalite-par-region.xlsx";
        File secFile = new File(secInput);
        if (secFile.exists()) {
            Dataset<Row> secRaw = DataIngestionUtils.loadAsDataset(spark, secFile);
            Dataset<Row> secGeo = DataCleaner.filterByBretagne(secRaw);
            Dataset<Row> secFiltered = DataCleaner.filterByYears(secGeo);
            Dataset<Row> secPivot = DataAggregator.pivotByYear(secFiltered);
            secPivot = DataAggregator.ensureGeoColumns(secPivot);
            String secOutDir = "src/main/resources/outputs/Security";
            new File(secOutDir).mkdirs();
            String secCsv = "criminalite-par-region.csv";
            CSVUtils.saveAsSingleCSV(secPivot, secOutDir, secCsv);
            CSVUtils.previewCsv(secOutDir + "/" + secCsv);
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
        
     // Gestion des communes niveau BDD
        File geoFile = new File("src/main/resources/inputs/FichierReferenceCommunesDepartement/communes-france.csv");
        MySQLConverter.loadAndWriteCommunesDepartement(spark, writer, geoFile);

        // — 1) On force ‘code_com’ en VARCHAR(5) pour pouvoir y mettre une PK
        writer.modifyColumnType(
            "Commune",
            "code_com",
            "VARCHAR(5) NOT NULL"
        );

        // — 2) Clés primaires et étrangères dimensionnelles
        writer.addPrimaryKey("Departement", "code_dep");
        writer.addPrimaryKey("Commune",     "code_com");
        writer.addForeignKey(
            "Commune", "fk_commune_dept",
            "code_dep",
            "Departement", "code_dep",
            "RESTRICT", "CASCADE"
        );

        // — 3) Chargement des faits (créera la table Donnees_commune)
        MySQLConverter.processOutputCSVs(spark, writer, "src/main/resources/outputs");
        MySQLConverter.processDepartementsCSVsOnCommunes(spark, writer, "src/main/resources/outputs");
        MySQLConverter.processRegionsCSVsOnCommunes(spark, writer, "src/main/resources/outputs");

        // — 4) Avant d’ajouter les FK sur Donnees_commune, on corrige les types
        writer.modifyColumnType(
            "Donnees_commune",
            "CODGEO",
            "VARCHAR(5) NOT NULL"
        );
        writer.modifyColumnType(
            "Donnees_commune",
            "Code département",
            "INT NOT NULL"
        );

        // — 4b) On s’assure du bon moteur et on crée les index nécessaires
        writer.execSql("ALTER TABLE `Donnees_commune` ENGINE = InnoDB");
        writer.addIndex("Donnees_commune", "idx_codgeo", "CODGEO");
        writer.addIndex("Donnees_commune", "idx_coddept", "Code département");

        // — 5) Maintenant on peut mettre les clés étrangères sur la table de faits
        writer.addForeignKey(
            "Donnees_commune", "fk_data_commune",
            "CODGEO",
            "Commune", "code_com",
            "RESTRICT", null
        );
        writer.addForeignKey(
            "Donnees_commune", "fk_data_dept",
            "Code département",
            "Departement", "code_dep",
            null, null
        );


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
        
     // … après avoir chargé toutes vos données BDD …
        String exportDir = "src/main/resources/exports/Donnees_commune";
        try {
            MySQLConverter.exportDonneesCommuneByYearAndType(
                spark, writer, exportDir
            );
        } catch (IOException e) {
            System.err.println("Erreur d’export CSV : " + e.getMessage());
        }


        spark.stop();
    }
}
