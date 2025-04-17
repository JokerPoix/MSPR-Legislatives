package legislativeMSPR;
import legislativeMSPR.Utils.CSVUtils;
import legislativeMSPR.Utils.DataIngestionUtils;
import legislativeMSPR.Utils.FileUtils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.io.File;
import java.util.List;
import java.util.Arrays;

public class Main {
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
	// Initialisation de Spark
    SparkSession spark = SparkSession.builder()
            .appName("rendementLoc$")
            .config("spark.master", "local[*]")  // Utilisation de tous les cœurs CPU disponibles
            .config("spark.driver.memory", "16g")  // Augmenter la mémoire allouée au driver
            .config("spark.executor.memory", "8g")  // Ajouter de la mémoire aux exécutors
            .config("spark.executor.cores", "4")  // Augmenter les cœurs d'exécution
            .config("spark.sql.shuffle.partitions", "16")  // Augmenter les partitions pour éviter la saturation de mémoire
            .getOrCreate();
    spark.sparkContext().setLogLevel("WARN");

    // Définition des chemins de fichiers
    String inputFinalResultDatasLeg  = "src/main/resources/inputs/FinalResultsDatasLeg";
    String outputFinalResultDatasLeg = "src/main/resources/outputs/FinalResultsDatasLeg";
	
    File baseDir = new File(inputFinalResultDatasLeg);
    for (File inFile : FileUtils.listInputFiles(baseDir)) {
        String year = inFile.getParentFile().getName();
        // lecture
        Dataset<Row> raw = DataIngestionUtils.loadAsDataset(spark, inFile);
        List<String> cols = Arrays.asList(raw.columns());
        boolean hasDept   = cols.contains("Code département");
        boolean hasRegion = cols.contains("Code région");

        if (!hasDept && !hasRegion) {
            System.out.println("[Main] Skipping “" + inFile.getName()
                             + "” (pas de colonne “Code département” ni “Code région”).");
            continue;
        }
        Dataset<Row> cleaned = DataCleaner.filterByBretagne(raw);
        // Agrégation : additionner les scores Gauche/Droite
        Dataset<Row> aggregated = DataAggregator.aggregateScores(
            spark, cleaned, inputFinalResultDatasLeg + "/"+year + "/"+ "ReferenceDroiteGauche.csv"
        );
        // sortie
        String outDir      = outputFinalResultDatasLeg + "/" + year;
        String outFileName = inFile.getName().replaceAll("\\.xlsx?$", ".csv");
        new File(outDir).mkdirs();
        CSVUtils.saveAsSingleCSV(aggregated, outDir, outFileName);
        // aperçu
        CSVUtils.previewCsv(outDir + "/" + outFileName);
    }
	}
}