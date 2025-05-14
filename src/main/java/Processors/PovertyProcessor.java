package Processors;

import java.io.File;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import legislativeMSPR.DataAggregator;
import legislativeMSPR.DataCleaner;
import legislativeMSPR.Utils.CSVUtils;
import legislativeMSPR.Utils.DataIngestionUtils;

/**
 * PovertyProcessor : calcule les taux de pauvreté par commune.
 */
public class PovertyProcessor {
    public static void process(SparkSession spark, String inputDir, String outputDir) {
        File file = new File(inputDir + "/niveau-pauvrete-par-commune.xlsx");
        if (!file.exists()) {
            System.out.println("[PovertyProcessor] Fichier introuvable : " + file.getPath());
            return;
        }
        Dataset<Row> raw = DataIngestionUtils.loadAsDataset(spark, file);
        Dataset<Row> geo = DataAggregator.ensureGeoColumns(raw);
        Dataset<Row> filtered = DataCleaner.filterByBretagne(geo);
        Dataset<Row> agg = DataAggregator.averageByCommune(filtered);
        CSVUtils.saveAsSingleCSV(agg, outputDir, "niveau-pauvrete-par-commune.csv");
    }
}