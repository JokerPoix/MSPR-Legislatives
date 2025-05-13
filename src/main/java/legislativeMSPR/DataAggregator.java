package legislativeMSPR;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataTypes;

import legislativeMSPR.Utils.DataFrameUtils;
import legislativeMSPR.Utils.DataIngestionUtils;
import scala.collection.JavaConverters;
import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.*;

import java.io.File;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DataAggregator {

  public static Dataset<Row> aggregateScores(SparkSession spark,
                                             Dataset<Row> ds,
                                             String refPath) {
    // 1) load your reference CSV or XLSX
	Dataset<Row> ref = DataIngestionUtils.loadAsDataset(spark, new File(refPath));


    // 2) trim whitespace on its column names
    String[] trimmed = Arrays.stream(ref.columns())
                             .map(String::trim)
                             .toArray(String[]::new);
    ref = ref.toDF(trimmed);

    
    // 3) find exactly your “Code” and “Orientation” columns
    List<String> refCols = Arrays.asList(ref.columns());
    String codeCol   = refCols.stream()
                              .filter(c -> c.equalsIgnoreCase("Code"))
                              .findFirst()
                              .orElseThrow(() -> 
                                  new IllegalArgumentException("No Code column in reference"));
    String orientCol = refCols.stream()
                              .filter(c -> c.equalsIgnoreCase("Orientation"))
                              .findFirst()
                              .orElseThrow(() -> 
                                  new IllegalArgumentException("No Orientation column in reference"));

    // 4) build + broadcast a Map<code,orientation>
    Map<String,String> map = new HashMap<>();
    for (Row r : ref.collectAsList()) {
      Object codeVal   = r.getAs(codeCol);
      Object orientVal = r.getAs(orientCol);
      if (codeVal!=null && orientVal!=null) {
        map.put(codeVal.toString(), orientVal.toString());
      }
    }
    Broadcast<Map<String,String>> bMap = spark.sparkContext()
        .broadcast(map, scala.reflect.ClassTag$.MODULE$.apply(Map.class));

    // 5) register two simple UDFs
    spark.udf().register("isGauche", (String code) ->
        bMap.value().getOrDefault(code,"")
                   .equalsIgnoreCase("Gauche"),
        DataTypes.BooleanType);
    spark.udf().register("isDroite", (String code) ->
        bMap.value().getOrDefault(code,"")
                   .equalsIgnoreCase("Droite"),
        DataTypes.BooleanType);

    // 6) for each “Nuance candidat X” find its “% Voix/exprimés X”
    List<String> cols = Arrays.asList(ds.columns());
    List<Column> leftExprs  = new ArrayList<>();
    List<Column> rightExprs = new ArrayList<>();

    for (String nu : cols) {
        if (nu.startsWith("Nuance candidat ")) {
            String idx   = nu.substring("Nuance candidat ".length());
            String pct   = "% Voix/exprimés " + idx;
            if (!cols.contains(pct)) continue;
            Column pctNum = DataFrameUtils.normalizePct(pct);
            leftExprs .add( when(callUDF("isGauche", col(nu)), pctNum).otherwise(lit(0)) );
            rightExprs.add( when(callUDF("isDroite", col(nu)), pctNum).otherwise(lit(0)) );
        }
    }

    // 7) sum for each camp
    Column sumG = lit(0);
    for (Column c : leftExprs)  sumG = sumG.plus(c);
    Column sumD = lit(0);
    for (Column c : rightExprs) sumD = sumD.plus(c);

    // 8) Score_Divers = 100 - sumG - sumD
    Column rawDivers = lit(100).minus(sumG).minus(sumD);

	 // on clamp à 0 si < 0, puis on arrondit à 2 décimales
	 Column diversClamped = when(rawDivers.lt(0), lit(0))
	                          .otherwise(rawDivers);
    // 9) attach columns
    Dataset<Row> withScores = ds
            .withColumn("Score_Gauche", round(sumG, 2))
            .withColumn("Score_Droite", round(sumD, 2))
            .withColumn("Score_Divers", round(diversClamped, 2));

 // Liste des colonnes qu'on veut explicitement garder
    Set<String> keepCols = new HashSet<>(Arrays.asList(
        "Score_Gauche",
        "Score_Droite",
        "Score_Divers",
        "Code de la commune",
        "Code département",
        "Libellé du département",
        "LIBGEO",
        "% Exp/Ins",
        "CODGEO"
        // colonnes importantes à ajouter
    ));

    Dataset<Row> finalDataset = DataFrameUtils.keepOnlyColumns(withScores, keepCols);
    List<String> cols1 = Arrays.asList(finalDataset.columns());
    if (cols1.contains("Code département")) {
        finalDataset = finalDataset
            .withColumn("Code département",
                col("Code département").cast("int").cast("string"));
    }
    if (cols1.contains("Code de la commune")) {
        finalDataset = finalDataset
            .withColumn("Code de la commune",
                col("Code de la commune").cast("int").cast("string"));
    }
    return finalDataset;
  }
  
  
  /**
   * Pour un Dataset de Criminalité (Code région, annee, indicateur, unite_de_compte, nombre, taux_pour_mille, insee_pop),
   * pivote uniquement sur la colonne "annee" pour obtenir, pour chaque clef :
   *   taux_pour_mille_<year>
   */
  public static Dataset<Row> pivotByYear(Dataset<Row> ds) {
      // Les années à pivoter
      List<Integer> yearsInt = Arrays.asList(2017, 2022, 2024);
      // Convertir en Seq<Object> pour Spark
      List<Object> yearsObj = yearsInt.stream().map(y -> (Object) y).collect(Collectors.toList());
      scala.collection.Seq<Object> yearSeq = JavaConverters
        .asScalaIteratorConverter(yearsObj.iterator())
        .asScala()
        .toSeq();

      // Clés de grouping
      String[] keys = new String[]{ "Code région", "indicateur", "unite de compte" };

      // Pivot sur taux_pour_mille
      Dataset<Row> byTaux = ds
        .groupBy(col(keys[0]), col(keys[1]), col(keys[2]))
        .pivot("annee", yearSeq)
        .agg(first(col("taux pour mille")));

      // Renommage des colonnes pivotées
      for (Integer y : yearsInt) {
        byTaux = byTaux.withColumnRenamed(y.toString(), "taux pour mille " + y);
      }

      return byTaux;
  }

  /**
   * Assure la présence et le bon format des colonnes géographiques :
   * - Code département à 2 caractères (padding à gauche)
   * - Code de la commune à 3 caractères (padding à gauche)
   * - CODGEO toujours à 5 caractères (2+3)
   *
   * Les décimales ".0" sont supprimées avant conversion.
   *
   * @param ds Dataset en entrée
   * @return Dataset avec colonnes géographiques formatées
   */
  public static Dataset<Row> ensureGeoColumns(Dataset<Row> ds) {
      List<String> cols = Arrays.asList(ds.columns());
      boolean hasGeo   = cols.contains("CODGEO");
      boolean hasDept  = cols.contains("Code département");
      boolean hasComm  = cols.contains("Code de la commune");

      // 0) Nettoyer et pad Code département
      if (hasDept) {
          ds = ds.withColumn(
              "Code département",
              lpad(
                  col("Code département").cast("int").cast("string"),
                  2,
                  "0"
              )
          );
      }
      // 0b) Nettoyer et pad Code de la commune
      if (hasComm) {
          ds = ds.withColumn(
              "Code de la commune",
              lpad(
                  col("Code de la commune").cast("int").cast("string"),
                  3,
                  "0"
              )
          );
      }

      // 1) Si CODGEO existe, supprimer ".0" éventuel puis pad à 5 caractères
      if (hasGeo) {
          ds = ds.withColumn(
              "CODGEO",
              lpad(
                  regexp_replace(col("CODGEO"), "\\.0$", ""),
                  5,
                  "0"
              )
          );
      }

      // 2) Construire CODGEO si absent mais dept + commune présents
      if (!hasGeo && hasDept && hasComm) {
          ds = ds.withColumn(
              "CODGEO",
              concat(
                  col("Code département"),
                  col("Code de la commune")
              )
          );
      }
      // 3) Reconstituer dept/commune si absent mais CODGEO présent
      else if (hasGeo && (!hasDept || !hasComm)) {
          if (!hasDept) {
              ds = ds.withColumn(
                  "Code département",
                  substring(col("CODGEO"), 1, 2)
              );
          }
          if (!hasComm) {
              ds = ds.withColumn(
                  "Code de la commune",
                  substring(col("CODGEO"), 3, 3)
              );
          }
      }

      return ds;
  }

  public static Dataset<Row> aggregateEducation(Dataset<Row> df) {
      // 1) repérer et caster en entier (Long) toutes les colonnes dpx_*_rpop2021
      for (String c : df.columns()) {
          if (c.matches("dpx_.*_rec[12]rpop2021")) {
              df = df.withColumn(c, col(c).cast(DataTypes.LongType));
          }
      }

      // suffixes finaux
      String suff16 = "_rec1rpop2021";
      String suff25 = "_rec2rpop2021";

      // 2) lister les colonnes 16–24 et 25+
      List<String> cols16 = Arrays.stream(df.columns())
                                  .filter(c -> c.endsWith(suff16))
                                  .collect(Collectors.toList());
      List<String> cols25 = Arrays.stream(df.columns())
                                  .filter(c -> c.endsWith(suff25))
                                  .collect(Collectors.toList());

      // 3) séparer universitaires vs bac_ou_moins
      Pattern univ = Pattern.compile(".*_rec[56].*");
      Pattern bac   = Pattern.compile(".*_rec[0-4].*");

      Column sumBac16  = cols16.stream()
                               .filter(bac.asPredicate())
                               .map(functions::col)
                               .reduce(lit(0L), Column::plus);
      Column sumUniv16 = cols16.stream()
                               .filter(univ.asPredicate())
                               .map(functions::col)
                               .reduce(lit(0L), Column::plus);
      Column sumBac25  = cols25.stream()
                               .filter(bac.asPredicate())
                               .map(functions::col)
                               .reduce(lit(0L), Column::plus);
      Column sumUniv25 = cols25.stream()
                               .filter(univ.asPredicate())
                               .map(functions::col)
                               .reduce(lit(0L), Column::plus);

      // 4) totaux
      Column pop16  = sumBac16.plus(sumUniv16);
      Column pop25  = sumBac25.plus(sumUniv25);
      Column popTot = pop16.plus(pop25);  // déjà LongType

      // 5) taux (double arrondi)
      Column pctUnivTot   = round(sumUniv16.plus(sumUniv25)
                                       .divide(popTot)
                                       .multiply(100), 2);
      Column pctBacTot    = round(sumBac16.plus(sumBac25)
                                       .divide(popTot)
                                       .multiply(100), 2);
      Column pctUniv16_24 = round(sumUniv16.divide(pop16).multiply(100), 2);
      Column pctBac16_24  = round(sumBac16.divide(pop16).multiply(100), 2);
      Column pctUniv25p   = round(sumUniv25.divide(pop25).multiply(100), 2);
      Column pctBac25p    = round(sumBac25.divide(pop25).multiply(100), 2);

      // 6) on enrichit df AVANT de trier les colonnes
      Dataset<Row> enriched = df
        .withColumn("population_totale",                  popTot)
        .withColumn("taux_diplome_universitaire_total",  pctUnivTot)
        .withColumn("taux_bac_ou_moins_total",           pctBacTot)
        .withColumn("taux_universitaire_16_24_ans",      pctUniv16_24)
        .withColumn("taux_bac_ou_moins_16_24_ans",       pctBac16_24)
        .withColumn("taux_universitaire_25_ans_et_plus", pctUniv25p)
        .withColumn("taux_bac_ou_moins_25_ans_et_plus",  pctBac25p);

      // 7) on ne garde QUE les colonnes finales (géographie + agrégats)
      return enriched.select(
          col("CODGEO"),
          col("Code département"),
          col("Code de la commune"),
          col("LIBGEO"),
          col("population_totale"),
          col("taux_diplome_universitaire_total"),
          col("taux_bac_ou_moins_total"),
          col("taux_universitaire_16_24_ans"),
          col("taux_bac_ou_moins_16_24_ans"),
          col("taux_universitaire_25_ans_et_plus"),
          col("taux_bac_ou_moins_25_ans_et_plus")
      );
 }
  public static Dataset<Row> averagePovertyByCommune(Dataset<Row> df) {
	    return df
	      .groupBy(col("CODGEO"), col("LIBGEO"))
	      .agg(
	        format_number(
	          avg(col("Taux de bas revenus déclarés au seuil de 60 %")),
	          2
	        ).alias("taux_pauvrete_commune")
	      );
	}
}

