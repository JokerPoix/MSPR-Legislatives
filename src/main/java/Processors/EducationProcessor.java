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
 * EducationProcessor : agrège la population selon le niveau d'étude.
 */
public class EducationProcessor {
    public static void process(SparkSession spark, String inputDir, String outputDir) {
        File file = new File(inputDir + "/population-selon-niveau-etude-par-commune.xlsx");
        if (!file.exists()) {
            System.out.println("[EducationProcessor] Fichier introuvable : " + file.getPath());
            return;
        }
        Dataset<Row> raw = DataIngestionUtils.loadAsDataset(spark, file);
        Dataset<Row> geo = DataAggregator.ensureGeoColumns(raw);
        Dataset<Row> filtered = DataCleaner.filterByBretagne(geo);
        Dataset<Row> agg = DataAggregator.aggregateEducation(filtered);
        CSVUtils.saveAsSingleCSV(agg, outputDir, "population-niveau-etude-par-commune.csv");
    }
}
