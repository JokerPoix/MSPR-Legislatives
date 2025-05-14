package legislativeMSPR.Utils;

import legislativeMSPR.MySqlWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Column;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

/**
 * Version refactorisée de MySQLConverter :
 * - Parcours générique des CSV
 * - Pivot, enrichissement, jointure, écriture
 * - Commentaires en français
 */
public class MySQLConverter {

	/**
     * Charge les tables de référence Communes et Départements.
     */
    public static void loadAndWriteCommunesDepartement(
            SparkSession spark,
            MySqlWriter writer,
            File geoCsv
    ) {
        // --- Préparation JDBC props avec désactivation des FK ---
        Properties jdbcProps = new Properties();
        jdbcProps.put("user", writer.getUser());
        jdbcProps.put("password", writer.getPassword());
        // Désactive FOREIGN_KEY_CHECKS pour toutes connexions Spark
        jdbcProps.put("sessionInitStatement", "SET FOREIGN_KEY_CHECKS=0");

        // --- 1) Lecture du CSV ---
        Dataset<Row> geo = DataIngestionUtils.loadAsDataset(spark, geoCsv);

        // --- 2) Création de la dimension Départements ---
        Dataset<Row> departements = geo
                .withColumn("code_dep", col("Code département").cast("int"))
                .withColumn("nom_dep",  col("Libellé département"))
                .select("code_dep", "nom_dep")
                .distinct();

        // --- 3) Création de la dimension Communes ---
        Dataset<Row> communes = geo
                .withColumn("code_com",  col("CODGEO").cast("string"))
                .withColumn("nom_com",   col("Libellé Commune"))
                .withColumn("geo_point", col("Geo Point"))
                .withColumn("code_dep",  col("Code département").cast("int"))
                .select("code_com", "nom_com", "geo_point", "code_dep");

        // --- 4) Write OVERWRITE Départements avec types SQL explicites ---
        departements.write()
                .mode(SaveMode.Overwrite)
                .option("createTableColumnTypes", "code_dep INT NOT NULL, nom_dep VARCHAR(255)")
                .jdbc(writer.getJdbcUrl(), "Departement", jdbcProps);

        // --- 5) Write OVERWRITE Communes avec types SQL explicites ---
        communes.write()
                .mode(SaveMode.Overwrite)
                .option("createTableColumnTypes",
                        "code_com VARCHAR(5) NOT NULL, " +
                        "nom_com VARCHAR(255), " +
                        "geo_point VARCHAR(100), " +
                        "code_dep INT NOT NULL")
                .jdbc(writer.getJdbcUrl(), "Commune", jdbcProps);

        // --- 6) Réactivation des FK pour connexions manuelles ---
        writer.execSql("SET FOREIGN_KEY_CHECKS=1");
    }



    /**
     * Méthode générique pour traiter des CSV d'outputs.
     */
    private static void processCSVs(SparkSession spark,
                                    MySqlWriter writer,
                                    String outputsDir,
                                    Predicate<File> fileFilter,
                                    BiFunction<Dataset<Row>, File, Dataset<Row>> enhancer) {
        Deque<File> stack = new ArrayDeque<>();
        stack.push(new File(outputsDir));

        while (!stack.isEmpty()) {
            File dir = stack.pop();
            File[] children = dir.listFiles();
            if (children == null) continue;
            for (File f : children) {
                if (f.isDirectory()) { stack.push(f); continue; }
                if (!f.getName().toLowerCase().endsWith(".csv") || !fileFilter.test(f)) continue;

                Dataset<Row> df = DataIngestionUtils.loadAsDataset(spark, f);
                Dataset<Row> enriched = enhancer.apply(df, f);

                // Si l'enrichissement n'a pas apporté 'annee' ou 'type', on saute ce fichier
                if (!Arrays.asList(enriched.columns()).contains("annee") ||
                    !Arrays.asList(enriched.columns()).contains("type")) {
                    continue;
                }

                Dataset<Row> dimCommune = loadDimensionCommune(spark, writer);
                Dataset<Row> joined = joinWithDimension(enriched, dimCommune);
                Dataset<Row> withJson = generateJsonColumn(joined);

                Dataset<Row> finalDf = withJson.select(
                    col("code_com").alias("CODGEO"),
                    col("nom_com").alias("LIBGEO"),
                    col("code_dep").alias("Code département"),
                    col("annee"),
                    col("type"),
                    col("data")
                );

                ensureFactTable(writer, finalDf, "Donnees_commune");
                finalDf.write()
                       .mode(SaveMode.Append)
                       .jdbc(writer.getJdbcUrl(), "Donnees_commune", writer.getConnectionProps());

                System.out.println("[MySQLConverter] Appended " + finalDf.count() + " rows from " + f.getPath());
            }
        }
    }

    /**
     * CSV par-commune et par-departement.
     */
    public static void processOutputCSVs(SparkSession spark,
                                         MySqlWriter writer,
                                         String outputsDir) {
        Predicate<File> filter = f -> {
            String name = f.getName().toLowerCase();
            String year = f.getParentFile().getName();
            return year.matches("\\d{4}") && (name.contains("par-commune.csv") || name.contains("par-departement.csv"));
        };
        BiFunction<Dataset<Row>, File, Dataset<Row>> enhancer = (df, f) -> df
            .withColumn("type", lit(f.getName().replaceAll("(?i)par-commune\\.csv$", "").replaceAll("(?i)par-departement\\.csv$", "").replace('-', ' ')))
            .withColumn("annee", lit(f.getParentFile().getName()));
        processCSVs(spark, writer, outputsDir, filter, enhancer);
    }

    /**
     * CSV par-region : pivot, cross-join.
     */
    public static void processRegionsCSVsOnCommunes(SparkSession spark,
                                                    MySqlWriter writer,
                                                    String outputsDir) {
        Predicate<File> filter = f -> f.getName().toLowerCase().endsWith("par-region.csv");
        BiFunction<Dataset<Row>, File, Dataset<Row>> enhancer = (df, f) -> {
            List<String> metrics = Arrays.stream(df.columns())
                .filter(c -> c.contains("%") || c.toLowerCase().contains("pour mille"))
                .collect(Collectors.toList());
            List<String> years = Arrays.stream(df.columns())
                .filter(c -> c.matches(".*\\d{4}$"))
                .collect(Collectors.toList());
            if (metrics.isEmpty() || years.isEmpty()) return df.limit(0);

            String typeVal = f.getName().replaceAll("(?i)par-region\\.csv$", "").replace('-', ' ').trim();
            String stackExpr = buildStackExpression(years, true);

            return df.select(
                col("Code région"),
                col("indicateur"),
                expr(stackExpr)
            ).withColumn("type", lit(typeVal))
             .withColumn("annee", col("annee"));
        };
        processCSVs(spark, writer, outputsDir, filter, enhancer);
    }

    /**
     * CSV par-departement métriques : pivot, replicate.
     */
    public static void processDepartementsCSVsOnCommunes(SparkSession spark,
                                                         MySqlWriter writer,
                                                         String outputsDir) {
        Predicate<File> filter = f -> f.getName().toLowerCase().endsWith("par-departement.csv");
        BiFunction<Dataset<Row>, File, Dataset<Row>> enhancer = (df, f) -> {
            List<String> years = Arrays.stream(df.columns())
                .filter(c -> c.matches(".*\\d{4}$"))
                .collect(Collectors.toList());
            if (years.isEmpty()) return df.limit(0);

            String typeVal = f.getName().replaceAll("(?i)par-departement\\.csv$", "").replace('-', ' ').trim();
            String stackExpr = buildStackExpression(years, false);

            return df.select(
                col("Code département"),
                expr(stackExpr)
            ).withColumn("type", lit(typeVal))
             .withColumn("annee", col("annee"));
        };
        processCSVs(spark, writer, outputsDir, filter, enhancer);
    }

    public static void exportDonneesCommuneByYearAndType(
            SparkSession spark,
            MySqlWriter writer,
            String outputBaseDir
    ) {
        System.out.println("[MySQLConverter] → Début export vers " + outputBaseDir);
        Dataset<Row> df;
        try {
            df = spark.read()
                .format("jdbc")
                .option("url", writer.getJdbcUrl())
                .option("dbtable", "Donnees_commune")
                .option("user", writer.getUser())
                .option("password", writer.getPassword())
                .load();
        } catch (Exception e) {
            System.err.println("[MySQLConverter] Échec connexion JDBC : " + e.getMessage());
            return;
        }

        long totalRows = df.count();
        System.out.println("[MySQLConverter] → Lignes chargées : " + totalRows);
        df.printSchema();

        List<Row> combos = df.select("annee","type")
                             .distinct()
                             .collectAsList();
        System.out.println("[MySQLConverter] → Combinaisons détectées : " + combos.size());
        if (combos.isEmpty()) {
            System.out.println("[MySQLConverter] → Aucune combinaison à traiter, arrêt de l'export.");
            return;
        }

        for (Row r : combos) {
            String annee = r.getString(0);
            String type  = r.getString(1);
            System.out.println(String.format(
                "[MySQLConverter] → Traitement %s / %s",
                annee, type
            ));

            Dataset<Row> slice = df.filter(
                col("annee").equalTo(annee)
                .and(col("type").equalTo(type))
            );
            Dataset<Row> flat = DynamicJsonFlattener.flattenMapColumn(slice, "data", "");

            String dirYear = outputBaseDir + "/" + annee;
            new File(dirYear).mkdirs();

            try {
                CSVUtils.saveAsSingleCSV(flat, dirYear, type + ".csv");
                long count = flat.count();  // déclenche l'action Spark
                System.out.println(String.format(
                    "[MySQLConverter] Exporté %d lignes vers %s/%s",
                    count, dirYear, type + ".csv"
                ));
            } catch (Exception e) {
                System.err.println(String.format(
                    "[MySQLConverter] Erreur export %s/%s : %s",
                    annee, type, e.getMessage()
                ));
            }
        }
    }

    

    private static Dataset<Row> loadDimensionCommune(SparkSession spark, MySqlWriter writer) {
        return spark.read().format("jdbc")
            .option("url", writer.getJdbcUrl())
            .option("dbtable", "Commune")
            .option("user", writer.getUser())
            .option("password", writer.getPassword())
            .load();
    }

    /**
     * Joint les données enrichies avec la dimension Commune.
     * Cas Commune: jointure sur CODGEO;
     * Cas Département: jointure sur Code département->code_dep;
     * Cas Région: réplique (cross join) sur toutes les communes.
     */
    private static Dataset<Row> joinWithDimension(Dataset<Row> enriched,
                                                  Dataset<Row> dimCommune) {
        List<String> cols = Arrays.asList(enriched.columns());
        if (cols.contains("CODGEO")) {
            // Fichier par-commune
            return enriched.join(
                dimCommune,
                enriched.col("CODGEO").equalTo(dimCommune.col("code_com")),
                "inner"
            );
        } else if (cols.contains("Code département")) {
            // Fichier par-département
            return enriched.join(
                dimCommune,
                enriched.col("Code département").equalTo(dimCommune.col("code_dep")),
                "inner"
            );
        } else if (cols.contains("Code région")) {
            // Fichier par-region: réplication sur toutes les communes
            return enriched.crossJoin(dimCommune);
        } else {
            // Aucun clé de dimension détectée
            throw new IllegalArgumentException("Clé de dimension introuvable dans enrichissement: " + cols);
        }
    }

    private static Dataset<Row> generateJsonColumn(Dataset<Row> joined) {
        List<String> exclude = Arrays.asList("CODGEO","LIBGEO","Code département","annee","type","Code région","indicateur");
        String[] cols = Arrays.stream(joined.columns()).filter(c->!exclude.contains(c)).toArray(String[]::new);
        return joined.withColumn("data", to_json(struct(Arrays.stream(cols).map(colName->col(colName)).toArray(Column[]::new))));
    }

    private static String buildStackExpression(List<String> yearCols, boolean includeIndi) {
        String expr = yearCols.stream().map(c -> {
            String year = c.substring(c.length()-4);
            String metric = c.replaceAll("\\s*\\d{4}$", "");
            return "'"+metric+"','"+year+"',`"+c+"`";
        }).collect(Collectors.joining(", "));
        return "stack("+yearCols.size()+", "+expr+") as (typeMetric, annee, valeur)";
    }

    private static void ensureFactTable(MySqlWriter writer, Dataset<Row> df, String tableName) {
        try (Connection conn = DriverManager.getConnection(writer.getJdbcUrl(), writer.getUser(), writer.getPassword())){
            DatabaseMetaData meta = conn.getMetaData();
            try(ResultSet rs=meta.getTables(null,null,tableName,null)){
                if(!rs.next()) df.limit(0).write().mode(SaveMode.Overwrite).jdbc(writer.getJdbcUrl(),tableName, writer.getConnectionProps());
            }
        } catch (SQLException e) {
            throw new RuntimeException("Erreur création table " + tableName, e);
        }
    }
}
