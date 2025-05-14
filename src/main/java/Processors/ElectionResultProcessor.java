package Processors;

import java.io.File;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import legislativeMSPR.DataAggregator;
import legislativeMSPR.DataCleaner;
import legislativeMSPR.Utils.CSVUtils;
import legislativeMSPR.Utils.DataIngestionUtils;
import legislativeMSPR.Utils.FileUtils;

/**
 * ElectionResultProcessor : traite les résultats électoraux par année.
 */
public class ElectionResultProcessor {
    /**
     * Charge les fichiers Excel, nettoie les données, agrège et exporte en CSV.
     *
     * @param spark SparkSession
     * @param inputDir Répertoire contenant les Excel par année
     * @param outputDir Répertoire de sortie des CSV
     */
    public static void process(SparkSession spark, String inputDir, String outputDir) {
        File base = new File(inputDir);
        for (File inFile : FileUtils.listInputFiles(base)) {
            String fileName = inFile.getName();
            // On ignore les fichiers de référence
            if (fileName.startsWith("Reference")) continue;
            String year = inFile.getParentFile().getName();
            // 1) Lecture Excel → DataFrame
            Dataset<Row> raw = DataIngestionUtils.loadAsDataset(spark, inFile);
            // 2) Normalisation des colonnes géographiques
            Dataset<Row> geo = DataAggregator.ensureGeoColumns(raw);
            // 3) Filtrage par région Bretagne
            Dataset<Row> filtered = DataCleaner.filterByBretagne(geo);
            // 4) Agrégation des scores Gauche/Droite
            String refFile = inputDir + "/" + year + "/ReferenceDroiteGauche.csv";
            Dataset<Row> result = DataAggregator.aggregateScores(spark, filtered, refFile);
            // 5) Export CSV
            CSVUtils.saveAsSingleCSV(result, outputDir + "/" + year, fileName.replaceAll("\\.xlsx?$", ".csv"));
        }
    }
}