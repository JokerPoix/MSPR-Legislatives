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
 * SecurityProcessor : traite la criminalité par région.
 */
public class SecurityProcessor {
    public static void process(SparkSession spark, String inputDir, String outputDir) {
        File file = new File(inputDir + "/criminalite-par-region.xlsx");
        if (!file.exists()) {
            System.out.println("[SecurityProcessor] Fichier introuvable : " + file.getPath());
            return;
        }
        Dataset<Row> raw = DataIngestionUtils.loadAsDataset(spark, file);
        Dataset<Row> geo = DataAggregator.ensureGeoColumns(raw);
        Dataset<Row> filtered = DataCleaner.filterByBretagne(geo);
        Dataset<Row> byYear = DataCleaner.filterByYears(filtered);
        Dataset<Row> pivot = DataAggregator.pivotByYear(byYear);
        CSVUtils.saveAsSingleCSV(pivot, outputDir, "criminalite-par-region.csv");
    }
}