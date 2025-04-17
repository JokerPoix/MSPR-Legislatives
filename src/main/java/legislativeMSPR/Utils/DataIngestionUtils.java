package legislativeMSPR.Utils;

import java.io.File;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataIngestionUtils {

    /**
     * Charge un fichier CSV ou XLSX en Dataset<Row>, selon son extension.
     * Repose sur spark.read().csv(..) ou spark-excel pour les .xlsx.
     *
     * @param spark   SparkSession en cours
     * @param inFile  fichier à charger
     * @return Dataset<Row> prêt à être transformé
     */
    public static Dataset<Row> loadAsDataset(SparkSession spark, File inFile) {
        String path = inFile.getAbsolutePath();
        if (inFile.getName().toLowerCase().endsWith(".csv")) {
            return spark.read()
                        .option("header", "true")
                        .option("delimiter", ";")
                        .csv(path);
        } else if (inFile.getName().toLowerCase().endsWith(".xlsx")) {
            return spark.read()
                        .format("com.crealytics.spark.excel")
                        .option("header", "true")
                        .option("inferSchema", "true")
                        .load(path);
        } else {
            throw new IllegalArgumentException("Format non supporté : " + inFile.getName());
        }
    }
}
