package legislativeMSPR.config;

import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class SparkContextProvider {
    public static SparkSession create() {
        // Réduction des logs verbeux
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        Logger.getRootLogger().setLevel(Level.WARN);
        
        return SparkSession.builder()
            .appName("LegislativeETL")
            .config("spark.master", "local[*]")
            .config("spark.driver.memory", "16g")
            .config("spark.executor.memory", "8g")
            .config("spark.executor.cores", "4")
            .config("spark.sql.shuffle.partitions", "16")
            .getOrCreate();
    }
}
