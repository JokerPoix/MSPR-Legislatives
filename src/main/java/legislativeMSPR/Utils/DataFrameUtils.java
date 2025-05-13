package legislativeMSPR.Utils;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.regexp_replace;

import java.util.Arrays;
import java.util.Set;

public class DataFrameUtils {

    /**
     * Garde uniquement les colonnes spécifiées et supprime le reste.
     *
     * @param ds le dataset d'origine
     * @param keepColumns les colonnes à conserver
     * @return un nouveau dataset avec uniquement les colonnes souhaitées
     */
    public static Dataset<Row> keepOnlyColumns(Dataset<Row> ds, Set<String> keepColumns) {
        // Récupère toutes les colonnes du DataFrame
        String[] allCols = ds.columns();

        // Construit la liste des colonnes à drop : celles qui ne sont pas dans keepColumns
        String[] toDrop = Arrays.stream(allCols)
            .filter(c -> !keepColumns.contains(c))
            .toArray(String[]::new);
            
        // Retourne le DataFrame en dropant les colonnes non désirées
        return ds.drop(toDrop);
    }
    
    /** 
     * Turn a “%”‑string into a Double: strip “%”, swap comma→dot, cast.
     */
    public static Column normalizePct(String colName) {
      return regexp_replace(
               regexp_replace(col(colName), "%", ""),
               ",", "."
             ).cast(DataTypes.DoubleType);
    }
    
}