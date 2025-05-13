package legislativeMSPR.Utils;

import legislativeMSPR.MySqlWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Column;

import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.map;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.to_json;
import static org.apache.spark.sql.functions.expr;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Charge et écrit en base MySQL les tables de référence Communes et Départements
 */
public class MySQLConverter {
	
	// TODO Refacto en une fonction pour faire 3 fonctions une pour les communes dans l'ensemble une autre pour les données niveau département etc
    /**
     * Charge le fichier CSV des communes et départements et écrit deux tables MySQL:
     * - Departement(code_dep INT, nom_dep VARCHAR)
     * - Commune(code_com VARCHAR, nom_com VARCHAR, geo_point VARCHAR, code_dep INT)
     *
     * @param spark     SparkSession actif
     * @param writer    MySqlWriter configuré
     * @param geoCsv   Chemin vers "communes‑france.csv"
     */
    public static void loadAndWriteCommunesDepartement(SparkSession spark, MySqlWriter writer, File geoCsv) {
        // 1) Lecture du fichier CSV
        Dataset<Row> geo = DataIngestionUtils.loadAsDataset(spark, geoCsv);

        // 2) Construction de la table Departement (distinct)
        Dataset<Row> departements = geo
            .withColumn("code_dep", col("Code département").cast("int"))
            .withColumn("nom_dep", col("Libellé département"))
            .select("code_dep", "nom_dep")
            .distinct();

        // 3) Construction de la table Commune
        Dataset<Row> communes = geo
            .withColumn("code_com", col("CODGEO").cast("int").cast("string"))
            .withColumn("nom_com", col("Libellé Commune"))
            .withColumn("geo_point", col("Geo Point"))
            .withColumn("code_dep", col("Code département").cast("int"))
            .select("code_com", "nom_com", "geo_point", "code_dep");

        // 4) Écriture en base MySQL
        writer.writeTable(departements, "Departement");
        writer.writeTable(communes,     "Commune");
    }
    
    /**
     * Parcourt récursivement le dossier outputs,
     * traite les CSV par-commune.csv et par-departement.csv,
     * et charge les données de faits dans Donnees_commune.
     */
    public static void processOutputCSVs(SparkSession spark,
                                         MySqlWriter writer,
                                         String outputsDir) {
        // 1) Démarrage du traitement
        File base = new File(outputsDir);
        Deque<File> stack = new ArrayDeque<>();
        stack.push(base);

        while (!stack.isEmpty()) {
            File dir = stack.pop();
            File[] files = dir.listFiles();
            if (files == null) {
                continue;
            }

            for (File f : files) {
                if (f.isDirectory()) {
                    stack.push(f);
                    continue;
                }

                String fname = f.getName().toLowerCase();
                if (!fname.endsWith(".csv")) {
                    continue;
                }

                File parent = f.getParentFile();
                String yearDir = parent.getName();
                if (!yearDir.matches("\\d{4}")) {
                    continue;
                }

                boolean isCommune = fname.contains("par-commune.csv");
                boolean isDept    = fname.contains("par-departement.csv");
                if (!isCommune && !isDept) {
                    continue;
                }

                // 2) Lecture du CSV et ajout des colonnes 'type' et 'annee'
                Dataset<Row> df = DataIngestionUtils
                    .loadAsDataset(spark, f)
                    .withColumn("type", lit(
                        f.getName()
                         .replaceAll("(?i)par-commune\\.csv$", "")
                         .replaceAll("(?i)par-departement\\.csv$", "")
                         .replace('-', ' ')
                    ))
                    .withColumn("annee", lit(yearDir));
                String factTable = "Donnees_commune";

                // 3) Chargement de la dimension 'Commune'
                Dataset<Row> dimCommune = spark
                    .read()
                    .format("jdbc")
                    .option("url",     writer.getJdbcUrl())
                    .option("dbtable", "Commune")
                    .option("user",    writer.getUser())
                    .option("password",writer.getPassword())
                    .load();

                // 4) Jointure sur CODGEO ou Code département
                Dataset<Row> joined;
                if (isCommune) {
                    joined = df.join(
                        dimCommune,
                        df.col("CODGEO").equalTo(dimCommune.col("code_com")),
                        "inner"
                    );
                } else {
                    joined = df.join(
                        dimCommune,
                        df.col("Code département").equalTo(dimCommune.col("code_dep")),
                        "inner"
                    );
                }

                // 5) Génération du champ 'data' JSON pour toutes les autres colonnes
                List<String> exclude = Arrays.asList(
                		"CODGEO", "LIBGEO", "Code département", "annee", "type"
                );
                String[] jsonCols = Arrays.stream(joined.columns())
                    .filter(c -> !exclude.contains(c))
                    .toArray(String[]::new);
                Dataset<Row> enriched = joined.withColumn(
                    "data",
                    to_json(
                        struct(
                            Arrays.stream(jsonCols)
                                  .map(name -> col(name))
                                  .toArray(Column[]::new)
                        )
                    )
                );

                // 6) Construction de finalDf en sélectionnant dims, méta et JSON
                Dataset<Row> finalDf = enriched.select(
                    col("code_com").alias("CODGEO"),
                    col("nom_com").alias("LIBGEO"),
                    col("code_dep").alias("Code département"),
                    col("annee"),
                    col("type"),
                    col("data")
                );

                ensureFactTable(writer, finalDf, "Donnees_commune");


                // 8) Insertion en mode Append
                finalDf
                    .write()
                    .mode(SaveMode.Append)
                    .jdbc(
                        writer.getJdbcUrl(),
                        factTable,
                        writer.getConnectionProps()
                    );
                System.out.println(
                    "[MySQLConverter] Append sur " + factTable +
                    ", lignes : " + finalDf.count()
                );
            }
        }
        
    }
    public static void processRegionsCSVsOnCommunes(
            SparkSession spark,
            MySqlWriter writer,
            String outputsDir) {
        File base = new File(outputsDir);
        Deque<File> stack = new ArrayDeque<>();
        stack.push(base);

        while (!stack.isEmpty()) {
            File dir = stack.pop();
            File[] files = dir.listFiles();
            if (files == null) continue;

            for (File file : files) {
                if (file.isDirectory()) {
                    stack.push(file);
                    continue;
                }
                if (!file.getName().toLowerCase().endsWith("par-region.csv")) {
                    continue;
                }

                // 1) Lecture brute du CSV
                Dataset<Row> df = DataIngestionUtils.loadAsDataset(spark, file);

                // 2) Détection des colonnes métriques
                List<String> metricCols = Arrays.stream(df.columns())
                    .filter(c -> c.contains("%") || c.toLowerCase().contains("pour mille"))
                    .collect(Collectors.toList());
                if (metricCols.isEmpty()) {
                    continue;
                }

                // 3) Extraction des années
                List<String> yearCols = Arrays.stream(df.columns())
                    .filter(c -> c.matches(".*\\d{4}$"))
                    .collect(Collectors.toList());
                if (yearCols.isEmpty()) {
                    continue;
                }

                // 4) Injection de la colonne "type" (nom du fichier sans suffixe)
                String typeVal = file.getName()
                    .replaceAll("(?i)par-region\\.csv$", "")
                    .replace('-', ' ')
                    .trim();
                Dataset<Row> withType = df.withColumn("type", lit(typeVal));

                // 5) Unpivot (type, annee, valeur)
                String stackExpr = "stack(" + yearCols.size() + ", " +
                    yearCols.stream()
                        .map(c -> {
                            String year = c.substring(c.length() - 4);
                            String metric = c.replaceAll("\\s*\\d{4}$", "");
                            return "'" + metric + "', '" + year + "', `" + c + "`";
                        })
                        .collect(Collectors.joining(", ")) +
                    ") as (typeMetric, annee, valeur)";

                Dataset<Row> pivoted = withType.select( col("Code région"),col("indicateur"),expr(stackExpr));


                // 6) Charger la dimension Commune
                Dataset<Row> dimCommune = spark.read()
                    .format("jdbc")
                    .option("url",    writer.getJdbcUrl())
                    .option("dbtable","Commune")
                    .option("user",   writer.getUser())
                    .option("password",writer.getPassword())
                    .load();

                // 7) Réplication sur toutes les communes (cross-join)
                Dataset<Row> joined = pivoted.crossJoin(dimCommune);

             // 8) Générer JSON incluant l’indicateur + la métrique
                Dataset<Row> withJson = joined.withColumn(
                    "data",
                    to_json(
                        map(
                            lit("indicateur"), col("indicateur"),  // "indicateur":"Cambriolages…"
                            col("typeMetric"),  col("valeur")     // "criminalite":"4.5…"  (typeVal = nom du CSV)
                        )
                    )
                );


                // 9) Sélection finale
                Dataset<Row> finalDf = withJson.select(
                    dimCommune.col("code_com").alias("CODGEO"),
                    dimCommune.col("nom_com").alias("LIBGEO"),
                    dimCommune.col("code_dep").alias("Code département"),
                    col("annee"),
                    lit(typeVal).alias("type"),
                    col("data")
                );

                // 10) Créer la table si besoin
                ensureFactTable(writer, finalDf, "Donnees_commune");

                // 11) Écriture en append
                finalDf.write()
                       .mode(SaveMode.Append)
                       .jdbc(
                           writer.getJdbcUrl(),
                           "Donnees_commune",
                           writer.getConnectionProps()
                       );
                System.out.println("[MySQLConverter] Append régions→communes, lignes : " + finalDf.count());
            }
        }
    }

    /**
     * Traite uniquement les fichiers "par-departement.csv" contenant des métriques (% ou "pour mille"),
     * pivote les colonnes année en lignes, et répète les données pour chaque commune.
     */
    public static void processDepartementsCSVsOnCommunes(
            SparkSession spark,
            MySqlWriter writer,
            String outputsDir) {
        File base = new File(outputsDir);
        Deque<File> stack = new ArrayDeque<>();
        stack.push(base);

        while (!stack.isEmpty()) {
            File dir = stack.pop();
            File[] files = dir.listFiles();
            if (files == null) continue;

            for (File file : files) {
                if (file.isDirectory()) {
                    stack.push(file);
                    continue;
                }
                if (!file.getName().toLowerCase().endsWith("par-departement.csv")) continue;

                // 1) Lire brut
                Dataset<Row> df = DataIngestionUtils.loadAsDataset(spark, file);

                // 2) Détection des métriques
                List<String> metricCols = Arrays.stream(df.columns())
                        .filter(c -> c.contains("%") || c.toLowerCase().contains("pour mille"))
                        .collect(Collectors.toList());
                if (metricCols.isEmpty()) continue;

                // 3) Extraction années
                List<String> yearCols = Arrays.stream(df.columns())
                		.filter(c -> c.matches(".*\\d{4}$"))
                        .collect(Collectors.toList());
                if (yearCols.isEmpty()) continue;

                // 4) Unpivot (type, annee, valeur)
                String stackExpr = "stack(" + yearCols.size() + ", " +
                        yearCols.stream()
                                .map(c -> {
                                    String year = c.substring(c.length() - 4);
                                    String typeName = c.replaceAll("\\s*\\d{4}$", "");
                                    return "'" + typeName + "', '" + year + "', `" + c + "`";
                                })
                                .collect(Collectors.joining(", ")) +
                        ") as (type, annee, valeur)";
                Dataset<Row> pivoted = df.select(
                        col("Code département"),
                        expr(stackExpr)
                );

                // 5) Joindre dimension Commune
                Dataset<Row> dimCommune = spark.read()
                        .format("jdbc")
                        .option("url",    writer.getJdbcUrl())
                        .option("dbtable","Commune")
                        .option("user",   writer.getUser())
                        .option("password",writer.getPassword())
                        .load();
                Dataset<Row> joined = pivoted.join(
                        dimCommune,
                        pivoted.col("Code département").equalTo(dimCommune.col("code_dep")),
                        "inner"
                );

                // 6) Générer JSON sur valeur (clé = libellé du type)
                Dataset<Row> withJson = joined.withColumn(
                        "data",
                        to_json(
                            map(
                                col("type"),
                                col("valeur")
                            )
                        )
                    );
                // 7) Sélection finale
                Dataset<Row> finalDf = withJson.select(
                        dimCommune.col("code_com").alias("CODGEO"),
                        dimCommune.col("nom_com").alias("LIBGEO"),
                        dimCommune.col("code_dep").alias("Code département"),
                        col("annee"),
                        col("type"),
                        col("data")
                );

                ensureFactTable(writer, finalDf, "Donnees_commune");

                // 8) Append
                finalDf.write()
                        .mode(SaveMode.Append)
                        .jdbc(
                                writer.getJdbcUrl(),
                                "Donnees_commune",
                                writer.getConnectionProps()
                        );
                System.out.println("[MySQLConverter] Append metrics pour communes, lignes : " + finalDf.count());
            }
        }
    }



    /**
     * Crée la table fact si elle n'existe pas en initialisant le schéma.
     */
    private static void ensureFactTable(
            MySqlWriter writer,
            Dataset<Row> df,
            String tableName) {
        try (Connection conn = DriverManager.getConnection(
                       writer.getJdbcUrl(),
                       writer.getUser(),
                       writer.getPassword())) {
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet rs = meta.getTables(null, null, tableName, null)) {
                if (!rs.next()) {
                    df.limit(0)
                      .write()
                      .mode(SaveMode.Overwrite)
                      .jdbc(
                              writer.getJdbcUrl(),
                              tableName,
                              writer.getConnectionProps()
                      );
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Erreur création table " + tableName, e);
        }
    }
}
