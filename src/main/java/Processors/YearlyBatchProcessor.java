package Processors;

import org.apache.spark.sql.SparkSession;

import legislativeMSPR.YearlyProcessor;

/**
 * YearlyBatchProcessor : exécute processByYear pour plusieurs jeux de données.
 */
public class YearlyBatchProcessor {
    public static void processAll(SparkSession spark, String[] datasets, String inputBase, String outputBase) {
        for (String ds : datasets) {
            String inDir  = inputBase + "/" + ds;
            String outDir = outputBase + "/" + ds;
            YearlyProcessor.processByYear(spark, inDir, outDir);
        }
    }
}