package legislativeMSPR;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;

import java.util.Arrays;

/**
 * Classe utilitaire pour filtrer les données selon les codes
 * de départements ou de régions de Bretagne et recaster
 * les colonnes en string pour éviter les “.0”.
 */
public class DataCleaner {

    /** Codes des départements de Bretagne. */
    private static final Integer[] BRETAGNE_DEPTS = {29, 22, 56, 35, 44};
    /** Codes des régions de Bretagne. */
    private static final Integer[] BRETAGNE_REG   = {53};

    /**
     * Filtre sur la colonne "Code département" si elle existe,
     * sinon sur "Code région", et recaste la colonne en string.
     * Si aucune n'existe, renvoie le Dataset inchangé.
     *
     * @param ds      Dataset initial
     * @return Dataset filtré & recasté
     */
    public static Dataset<Row> filterByBretagne(Dataset<Row> ds) {
        // priorité au département
        if (Arrays.asList(ds.columns()).contains("Code département")) {
            return ds
                .withColumn("Code département", col("Code département").cast("int"))
                .filter(col("Code département").isin((Object[]) BRETAGNE_DEPTS))
                .withColumn("Code département", col("Code département").cast("string"));
        }
        // sinon on regarde la région
        else if (Arrays.asList(ds.columns()).contains("Code région")) {
            return ds
                .withColumn("Code région", col("Code région").cast("int"))
                .filter(col("Code région").isin((Object[]) BRETAGNE_REG))
                .withColumn("Code région", col("Code région").cast("string"));
        }
        // aucune des deux colonnes : on ne filtre pas
        else {
            System.out.println("[DataCleaner] Ni 'Code département' ni 'Code région' présents, aucun filtrage appliqué.");
            return ds;
        }
    }
}
