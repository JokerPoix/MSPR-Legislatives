package legislativeMSPR;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.Seq;
import scala.collection.JavaConverters;
import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.col;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class DataAggregator {

  /** 
   * Turn a “%”‑string into a Double: strip “%”, swap comma→dot, cast.
   */
  private static Column normalizePct(String colName) {
    return regexp_replace(
             regexp_replace(col(colName), "%", ""),
             ",", "."
           ).cast(DataTypes.DoubleType);
  }

  public static Dataset<Row> aggregateScores(SparkSession spark,
                                             Dataset<Row> ds,
                                             String refPath) {
    // 1) load your reference CSV or XLSX
    Dataset<Row> ref;
    if (refPath.toLowerCase().endsWith(".csv")) {
      ref = spark.read()
                 .option("header","true")
                 .option("inferSchema","true")
                 .option("delimiter",",")
                 .csv(refPath);
    } else {
      ref = spark.read()
                 .format("com.crealytics.spark.excel")
                 .option("header","true")
                 .option("inferSchema","true")
                 .load(refPath);
    }

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
            Column pctNum = normalizePct(pct);
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

    // construire dynamiquement la liste des colonnes à drop
    String[] toDrop = Arrays.stream(withScores.columns())
    		.filter(c ->
            c.startsWith("Nuance candidat ") ||
            c.startsWith("Voix") ||
            c.startsWith("Sièges") ||
            c.startsWith("Nuance candidat ") ||
            c.startsWith("% Voix") ||
            c.startsWith("Numéro de panneau ") ||
            c.startsWith("Nom candidat ") ||
            c.startsWith("N°Panneau") ||
            c.startsWith("Nom") ||
            c.startsWith("Prénom") ||
            c.startsWith("Sexe") ||
            c.startsWith("Elu ") ||
            c.startsWith("Inutile") ||
            c.startsWith("Etat saisie")
    		)
            .toArray(String[]::new);

    return withScores.drop(toDrop);
  }
  
  
  /**
   * Pour un Dataset de Criminalité (Code région, annee, indicateur, unite_de_compte, nombre, taux_pour_mille, insee_pop),
   * pivote sur la colonne "annee" pour obtenir, pour chaque clef :
   *   nombre_<year>, taux_pour_mille_<year>, insee_pop_<year>
   */
  public static Dataset<Row> pivotByYear(Dataset<Row> ds) {
	    // 1) liste des années
	    List<Integer> yearsInt = Arrays.asList(2017, 2022, 2024);
	    // 2) on caste en Object pour obtenir Seq<Object>
	    List<Object> yearsObj = yearsInt.stream()
	      .map(y -> (Object) y)
	      .collect(Collectors.toList());
	    Seq<Object> yearSeq = JavaConverters
	      .asScalaIteratorConverter(yearsObj.iterator())
	      .asScala()
	      .toSeq();

	    // clés de grouping
	    String[] keys = new String[]{ "Code région", "indicateur", "unite_de_compte" };
	    // conversion des clés en Seq<String>
	    Seq<String> keySeq = JavaConverters
	      .asScalaIteratorConverter(Arrays.asList(keys).iterator())
	      .asScala()
	      .toSeq();

	    // pivot "nombre"
	    Dataset<Row> byNombre = ds
	      .groupBy(col(keys[0]), col(keys[1]), col(keys[2]))
	      .pivot("annee", yearSeq)
	      .agg(first("nombre"));
	    for (Integer y : yearsInt) {
	      byNombre = byNombre.withColumnRenamed(y.toString(), "nombre_" + y);
	    }

	    // pivot "taux_pour_mille"
	    Dataset<Row> byTaux = ds
	      .groupBy(col(keys[0]), col(keys[1]), col(keys[2]))
	      .pivot("annee", yearSeq)
	      .agg(first("taux_pour_mille"));
	    for (Integer y : yearsInt) {
	      byTaux = byTaux.withColumnRenamed(y.toString(), "taux_pour_mille_" + y);
	    }

	    // pivot "insee_pop"
	    Dataset<Row> byPop = ds
	      .groupBy(col(keys[0]), col(keys[1]), col(keys[2]))
	      .pivot("annee", yearSeq)
	      .agg(first("insee_pop"));
	    for (Integer y : yearsInt) {
	      byPop = byPop.withColumnRenamed(y.toString(), "insee_pop_" + y);
	    }

	    // join final sur les mêmes clés
	    return byNombre
	      .join(byTaux, keySeq, "inner")
	      .join(byPop,  keySeq, "inner");
	  }
}
