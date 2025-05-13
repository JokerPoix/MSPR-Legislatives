package legislativeMSPR;


import legislativeMSPR.Utils.CSVUtils;
import legislativeMSPR.Utils.DataIngestionUtils;
import legislativeMSPR.Utils.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.util.List;

public class YearlyProcessor {

    /**
     * Parcourt une arborescence input organisée par année, applique le même
     * flux (load → ensureGeo → filterBretagne → save CSV), et stocke
     * dans un dossier miroir.
     *
     * @param spark      SparkSession actif
     * @param baseInput  chemin du dossier de base (ex. "…/inputs/CarteLoyer")
     * @param baseOutput chemin du dossier de sortie (ex. "…/outputs/CarteLoyer")
     */
    public static void processByYear(
            SparkSession spark,
            String baseInput,
            String baseOutput
    ) {
        String featureName = new File(baseInput).getName();

        File outBase = new File(baseOutput);
        if (!outBase.exists() && !outBase.mkdirs()) {
            throw new RuntimeException("Impossible de créer le dossier " + baseOutput);
        }

        File inBase = new File(baseInput);
        if (!inBase.exists()) {
            System.err.println("[YearlyProcessor] Dossier introuvable : " + baseInput);
            return;
        }

        for (File yearDir : inBase.listFiles(File::isDirectory)) {
            String year = yearDir.getName();
            System.out.println("[YearlyProcessor] Traitement " + featureName + " pour " + year);

            File outYear = new File(outBase, year);
            if (!outYear.exists() && !outYear.mkdirs()) {
                System.err.println("[YearlyProcessor] Impossible de créer : " + outYear);
                continue;
            }

            List<File> inputs = FileUtils.listInputFiles(yearDir);
            if (inputs.isEmpty()) {
                System.out.println("[YearlyProcessor] Aucun fichier de " 
                    + featureName + " trouvé pour " + year);
                continue;
            }

            for (File f : inputs) {
                System.out.println("→ Traitement fichier : " + f.getPath());
                Dataset<Row> raw   = DataIngestionUtils.loadAsDataset(spark, f);
                Dataset<Row> geo   = DataAggregator.ensureGeoColumns(raw);
                Dataset<Row> clean = DataCleaner.filterByBretagne(geo);

                String outCsv = f.getName().replaceFirst("(?i)\\.xlsx?$", ".csv");
                CSVUtils.saveAsSingleCSV(clean, outYear.getPath(), outCsv);
                CSVUtils.previewCsv(outYear.getPath() + "/" + outCsv);
            }
        }
    }
}
