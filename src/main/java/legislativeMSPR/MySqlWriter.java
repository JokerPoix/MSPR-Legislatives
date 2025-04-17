package legislativeMSPR;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class MySqlWriter {
    private final String jdbcUrl;
    private final Properties connectionProps;

    /**
     * @param host      ex. "localhost"
     * @param port      ex. "3306"
     * @param database  ex. "MSPRLegislative"
     * @param user      ex. "poix"
     * @param password  ex. "poix"
     */
    public MySqlWriter(String host, String port, String database, String user, String password) {
        // on active allowPublicKeyRetrieval pour éviter l’erreur SHA2+clé publique
        this.jdbcUrl = String.format(
            "jdbc:mysql://%s:%s/%s?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true",
            host, port, database
        );
        this.connectionProps = new Properties();
        connectionProps.put("user", user);
        connectionProps.put("password", password);
        connectionProps.put("driver", "com.mysql.cj.jdbc.Driver");
    }

    /** Écrase la table si elle existe, sinon la crée. */
    public void writeTable(Dataset<Row> df, String tableName) {
        df.write()
          .mode(SaveMode.Overwrite)
          .jdbc(jdbcUrl, tableName, connectionProps);
    }
    
    /** Vide la base en droppant toutes les tables existantes */
    public void dropAllTables() {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW TABLES")) {

            // 1) On collecte d'abord tous les noms de tables
            List<String> tables = new ArrayList<>();
            while (rs.next()) {
                tables.add(rs.getString(1));
            }
            // 2) Puis on les droppe une à une
            for (String table : tables) {
                stmt.executeUpdate("DROP TABLE IF EXISTS `" + table + "`");
                System.out.println("→ Table MySQL supprimée : " + table);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Erreur en vidant la base MySQL", e);
        }
    }
}
