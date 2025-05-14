package Processors;

import java.io.File;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import legislativeMSPR.DataAggregator;
import legislativeMSPR.DataCleaner;
import legislativeMSPR.DataDispatcher;
import legislativeMSPR.Utils.CSVUtils;
import legislativeMSPR.Utils.DataIngestionUtils;

/**
 * SocialPositionProcessor : traite l'indice de position sociale au lycée par commune.
 * Lecture du fichier Excel, filtrage géographique, normalisation et agrégation,
 * puis export en CSV.
 */
public class SocialPositionProcessor {
    public static void process(SparkSession spark, String inputDir, String outputDir) {
        // 1) Chargement du fichier Excel
        File file = new File(inputDir + "/indice-position-sociale-lycee-par-commune.xlsx");
        if (!file.exists()) {
            System.out.println("[SocialPositionProcessor] Fichier introuvable : " + file.getPath());
            return;
        }

        // 2) Lecture brute en Dataset
        Dataset<Row> raw = DataIngestionUtils.loadAsDataset(spark, file);

        // 3) Ajout des colonnes géographiques standard (Code département, Code de la commune, CODGEO)
        Dataset<Row> geo = DataAggregator.ensureGeoColumns(raw);

        // 4) Filtrage pour ne conserver que la Bretagne
        Dataset<Row> filtered = DataCleaner.filterByBretagne(geo);

        // 5) Normalisation de la colonne 'annee' si nécessaire
        Dataset<Row> normalized = DataCleaner.normalizeYearColumn(filtered);

      
        // 6) Agrégation de l'indice de position sociale
        Dataset<Row> agg = DataAggregator.averageByCommune(normalized);

        // 7) Export du CSV unique
        CSVUtils.saveAsSingleCSV(
            agg,
            outputDir,
            "indice-position-sociale-lycee-par-commune.csv"
        );

        // 8) Dispatch des fichiers par année
        DataDispatcher.dispatchByYear(
            spark,
            outputDir,   // dossier où vient d'être écrit le CSV
            outputDir    // base de sortie : il créera outputDir/2019/…, outputDir/2022/…, etc.
        );
    }
}
