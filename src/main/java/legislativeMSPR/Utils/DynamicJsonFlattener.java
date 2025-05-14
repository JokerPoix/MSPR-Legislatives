package legislativeMSPR.Utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.types.DataTypes;
import java.util.List;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.map_keys;
import static org.apache.spark.sql.functions.explode;

/**
 * Utility to dynamically flatten a JSON column "mapCol" into separate columns.
 */
public class DynamicJsonFlattener {

    /**
     * Parses the JSON string column `mapCol` as a Map<String,String>,
     * then creates one column per key, named `prefix + safeKey`.
     *
     * @param df      Input DataFrame
     * @param mapCol  Name of the JSON column to parse
     * @param prefix  Prefix to add to generated column names
     * @return        DataFrame with new columns for each JSON key and original JSON column removed
     */
    public static Dataset<Row> flattenMapColumn(
            Dataset<Row> df,
            String mapCol,
            String prefix
    ) {
        // 1) Parse JSON string into MapType(String,String)
        Dataset<Row> withMap = df.withColumn(
            "m",
            from_json(
                col(mapCol),
                DataTypes.createMapType(
                    DataTypes.StringType,
                    DataTypes.StringType
                )
            )
        );

        // 2) Extract all distinct keys from the map
        List<String> keys = withMap
            .select(explode(map_keys(col("m"))).alias("key"))
            .distinct()
            .as(Encoders.STRING())
            .collectAsList();

        // 3) For each key, add a new safe-named column using map's value
        for (String key : keys) {
            String safe = prefix + key
                .toLowerCase()
                .replaceAll("[^a-z0-9]", "_");
            withMap = withMap.withColumn(
                safe,
                col("m").getItem(key)
            );
        }

        // 4) Drop the temporary map column and original JSON column
        return withMap.drop("m").drop(mapCol);
    }
}
