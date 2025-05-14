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
        var writer = df.write()
                       .mode(SaveMode.Overwrite);

        // Si on crée Departement ou Commune, on précise le DDL de chaque colonne
        if ("Departement".equals(tableName)) {
            writer = writer.option(
                "createTableColumnTypes",
                "code_dep INT, nom_dep VARCHAR(100)"
            );
        } else if ("Commune".equals(tableName)) {
            writer = writer.option(
                "createTableColumnTypes",
                // Les codes INSEE font toujours 5 caractères
                "code_com VARCHAR(5), nom_com VARCHAR(255), geo_point VARCHAR(255), code_dep INT"
            );
        }

        writer
          .jdbc(jdbcUrl, tableName, connectionProps);
    }

    
    /** Vide la base en droppant toutes les tables existantes, même si des FK pointent dessus */
    public void dropAllTables() {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps);
             Statement stmt = conn.createStatement()) {

            // 1) Désactivation des contraintes FK AVANT la requête SHOW TABLES
            stmt.execute("SET FOREIGN_KEY_CHECKS = 0");

            // 2) Récupération des tables dans un ResultSet à part
            List<String> tables = new ArrayList<>();
            try (ResultSet rs = stmt.executeQuery("SHOW TABLES")) {
                while (rs.next()) {
                    tables.add(rs.getString(1));
                }
            }

            // 3) Drop de chaque table
            for (String table : tables) {
                stmt.executeUpdate("DROP TABLE IF EXISTS `" + table + "`");
                System.out.println("→ Table MySQL supprimée : " + table);
            }

            // 4) Réactivation des contraintes FK
            stmt.execute("SET FOREIGN_KEY_CHECKS = 1");

        } catch (SQLException e) {
            throw new RuntimeException("Erreur en vidant la base MySQL", e);
        }
    }


    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getUser() {
        return connectionProps.getProperty("user");
    }

    public String getPassword() {
        return connectionProps.getProperty("password");
    }

	public Properties getConnectionProps() {
		return connectionProps;
	}
	
	public void execSql(String sql) {
	    try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps);
	         Statement stmt = conn.createStatement()) {
	        stmt.executeUpdate(sql);
	    } catch (SQLException e) {
	        throw new RuntimeException("Erreur SQL sur : " + sql, e);
	    }
	}
	/**
	 * Ajoute une clé primaire sur `tableName(colName)` si elle n'existe pas déjà.
	 */
	public void addPrimaryKey(String tableName, String colName) {
	    String sql = String.format(
	        "ALTER TABLE `%s` ADD PRIMARY KEY (`%s`)",
	        tableName, colName
	    );
	    execSql(sql);
	}

	/**
	 * Ajoute une contrainte de clé étrangère.
	 *
	 * @param table       table qui contient la FK
	 * @param fkName      nom de la contrainte
	 * @param colName     colonne locale
	 * @param refTable    table référencée
	 * @param refColName  colonne référencée
	 * @param onDelete    option ON DELETE (ex. CASCADE ou RESTRICT) ou null
	 * @param onUpdate    option ON UPDATE ou null
	 */
	public void addForeignKey(
	    String table, String fkName,
	    String colName,
	    String refTable, String refColName,
	    String onDelete, String onUpdate
	) {
	    StringBuilder sb = new StringBuilder();
	    sb.append("ALTER TABLE `").append(table).append("` ")
	      .append("ADD CONSTRAINT `").append(fkName).append("` ")
	      .append("FOREIGN KEY(`").append(colName).append("`) ")
	      .append("REFERENCES `").append(refTable).append("`(`").append(refColName).append("`)");
	    if (onDelete != null)  sb.append(" ON DELETE ").append(onDelete);
	    if (onUpdate != null)  sb.append(" ON UPDATE ").append(onUpdate);
	    execSql(sb.toString());
	}
	
	public void initializeSchema(Dataset<Row> departements, Dataset<Row> communes) {
	    // 1. (Re)création des tables
	    writeTable(departements, "Departement");
	    writeTable(communes,     "Commune");
	    // 2. Clés primaires
	    addPrimaryKey("Departement", "code_dep");
	    addPrimaryKey("Commune",     "code_com");
	    // 3. Clés étrangères
	    addForeignKey(
	        "Commune", "fk_commune_dept",
	        "code_dep",
	        "Departement", "code_dep",
	        "RESTRICT", "CASCADE"
	    );
	    // 4. (Plus tard, une fois Donnees_commune créée)
	    addForeignKey(
	        "Donnees_commune", "fk_data_commune",
	        "CODGEO",
	        "Commune", "code_com",
	        "RESTRICT", null
	    );
	    addForeignKey(
	        "Donnees_commune", "fk_data_dept",
	        "Code département",
	        "Departement", "code_dep",
	        null, null
	    );
	}
	/**
	 * Modifie le type SQL d'une colonne existante.
	 */
	public void modifyColumnType(String table, String column, String sqlType) {
	    String ddl = String.format(
	      "ALTER TABLE `%s` MODIFY `%s` %s",
	      table, column, sqlType
	    );
	    execSql(ddl);
	}
	/**
	 * Ajoute un index non-unique sur une colonne.
	 */
	public void addIndex(String table, String indexName, String column) {
	    String ddl = String.format(
	        "ALTER TABLE `%s` ADD INDEX `%s` (`%s`)",
	        table, indexName, column
	    );
	    execSql(ddl);
	}
	 /**
     * Active ou désactive la vérification des clés étrangères.
     * @param enabled true pour SET FOREIGN_KEY_CHECKS=1, false pour 0
     */
    public void setForeignKeyChecks(boolean enabled) {
        String val = enabled ? "1" : "0";
        execSql("SET FOREIGN_KEY_CHECKS = " + val);
    }

}
