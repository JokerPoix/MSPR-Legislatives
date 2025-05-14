package Processors;

import java.io.File;
import java.nio.file.Paths;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import legislativeMSPR.DataAggregator;
import legislativeMSPR.Utils.CSVUtils;
import legislativeMSPR.Utils.DataIngestionUtils;
import legislativeMSPR.Utils.FileUtils;

/**
 * ReadyForDBProcessor : convertit tous les fichiers Excel restants en CSV.
 */
public class ReadyForDBProcessor {
    public static void process(SparkSession spark, String inputDir, String outputDir) {
        File base = new File(inputDir);
        for (File file : FileUtils.listInputFiles(base)) {
            if (!file.getName().toLowerCase().matches(".*\\.xlsx?")) continue;
            String rel = base.toPath().relativize(file.toPath()).toString();
            String outPath = outputDir + "/" + rel.replaceFirst("(?i)\\.xlsx?$", ".csv");
            new File(Paths.get(outPath).getParent().toString()).mkdirs();
            Dataset<Row> raw = DataIngestionUtils.loadAsDataset(spark, file);
            Dataset<Row> geo = DataAggregator.ensureGeoColumns(raw);
            CSVUtils.saveAsSingleCSV(geo, Paths.get(outPath).getParent().toString(), Paths.get(outPath).getFileName().toString());
        }
    }
}