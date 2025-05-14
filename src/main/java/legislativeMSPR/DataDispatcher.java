package legislativeMSPR;


import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

import legislativeMSPR.Utils.CSVUtils;
import legislativeMSPR.Utils.DataIngestionUtils;

/**
 * DataDispatcher : sépare un CSV selon la colonne 'annee' et
 * écrit chaque sous-ensemble dans un dossier nommé par l'année,
 * en conservant le nom de fichier et les colonnes d'origine.
 */
public class DataDispatcher {

    /**
     * Pour chaque fichier CSV du répertoire inputDir,
     * charge le DataFrame, normalise la colonne 'annee',
     * puis écrit, pour chaque année distincte, un fichier CSV
     * dans outputBaseDir/<annee>/<nomFichier>.csv
     *
     * @param spark          SparkSession actif
     * @param inputDir       chemin vers le dossier contenant les CSV en entrée
     * @param outputBaseDir  chemin vers le dossier racine de sortie
     */
    public static void dispatchByYear(SparkSession spark,
                                      String inputDir,
                                      String outputBaseDir) {
        File inFolder = new File(inputDir);
        File[] files = inFolder.listFiles(f -> f.isFile() && f.getName().toLowerCase().endsWith(".csv"));
        if (files == null) {
            System.out.println("[DataDispatcher] Aucun fichier CSV trouvé dans " + inputDir);
            return;
        }

        for (File file : files) {
            // Charger le CSV brut
            Dataset<Row> df = DataIngestionUtils.loadAsDataset(spark, file);
            // Normaliser 'annee' (ex. "2019-2020" -> 2019)
            Dataset<Row> normalized = DataCleaner.normalizeYearColumn(df);
            // Récupérer les années distinctes
            List<Integer> years = normalized.select("annee")
                .distinct()
                .collectAsList()
                .stream()
                .map(r -> r.getInt(0))
                .collect(Collectors.toList());

            // Pour chaque année, filtrer et écrire un CSV
            for (Integer year : years) {
                Dataset<Row> slice = normalized.filter(col("annee").equalTo(year));
                // Création du dossier de sortie par année
                String yearDir = outputBaseDir + "/" + year;
                new File(yearDir).mkdirs();
                // Écriture en un seul fichier CSV, conserve le nom d'origine
                CSVUtils.saveAsSingleCSV(
                    slice,
                    yearDir,
                    file.getName()
                );
                System.out.println(String.format(
                    "[DataDispatcher] Écrit %d lignes pour %d dans %s/%s",
                    slice.count(), year, yearDir, file.getName()
                ));
            }
         // Suppression du fichier original après dispatch
            if (!file.delete()) {
                System.out.println("[DataDispatcher] Impossible de supprimer le fichier original : " + file.getPath());
            } else {
                System.out.println("[DataDispatcher] Fichier original supprimé : " + file.getPath());
            }
        }
    }
}

