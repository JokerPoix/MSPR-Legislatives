package legislativeMSPR;

import legislativeMSPR.config.SparkContextProvider;
import Processors.ElectionResultProcessor;
import Processors.YearlyBatchProcessor;
import Processors.PovertyProcessor;
import Processors.EducationProcessor;
import Processors.SecurityProcessor;
import Processors.SocialPositionProcessor;
import Processors.ReadyForDBProcessor;
import legislativeMSPR.Utils.MySQLConverter;
import java.io.File;

import org.apache.spark.sql.SparkSession;

/**
 * Classe Main : point d'entrée de l'application. Elle orchestre deux pipelines
 * distincts : 1) CSVPipeline pour la préparation des fichiers CSV 2)
 * DatabasePipeline pour l'injection dans la base MySQL
 */
public class Main {
	public static void main(String[] args) {
		SparkSession spark = SparkContextProvider.create();
        MySqlWriter writer = new MySqlWriter("localhost","3306","MSPRLegislative","poix","poix");
		// 1) On vide la base
        writer.dropAllTables();

        // 2) On génère les CSV
        CSVPipeline.runAll(spark);

        // 3) On exécute tout le pipeline base+export
        DatabasePipeline.runAll(spark);

        spark.stop();
    }
}

/**
 * CSVPipeline : classe utilitaire pour exécuter tous les traitements de
 * conversion / nettoyage / export de données en fichiers CSV.
 */
class CSVPipeline {
	/**
	 * Exécute l'ensemble des processors définis pour générer les CSV prêts à être
	 * chargés en base.
	 *
	 * @param spark SparkSession
	 */
	public static void runAll(SparkSession spark) {
		// 1) Résultats électoraux : Excel → CSV
		ElectionResultProcessor.process(spark, "src/main/resources/inputs/FinalResultsDatasLeg",
				"src/main/resources/outputs/FinalResultsDatasLeg");

		// 2) Traitements annuels (loyer, énergie, unité urbaine)
		// pour chaque répertoire passé en paramètre
		YearlyBatchProcessor.processAll(spark, new String[] { "CarteLoyer", "ProductionEnergetique", "UniteUrbaine" },
				"src/main/resources/inputs", "src/main/resources/outputs");

		// 3) Pauvreté par commune (2017)
		PovertyProcessor.process(spark, "src/main/resources/inputs/Pauvrete/2017",
				"src/main/resources/outputs/Pauvrete/2017");

		// 4) Niveau d'étude par commune (2022)
		EducationProcessor.process(spark, "src/main/resources/inputs/Demographie-NiveauEtude/2022",
				"src/main/resources/outputs/Demographie-NiveauEtude/2022");

		// 5) Données de sécurité par région
		SecurityProcessor.process(spark, "src/main/resources/inputs/Security", "src/main/resources/outputs/Security");

		// 6) Conversion générique Excel → CSV pour ReadyForDataBase
		ReadyForDBProcessor.process(spark, "src/main/resources/inputs/ReadyForDataBase",
				"src/main/resources/outputs/ReadyForDataBaseCsv");

		// 6) Conversion générique Excel → CSV pour ReadyForDataBase
		SocialPositionProcessor.process(spark, "src/main/resources/inputs/IndicePositionSociale",
				"src/main/resources/outputs/IndicePositionSociale");
	}
}

/**
 * DatabasePipeline : classe utilitaire pour configurer la base MySQL, créer les
 * dimensions, charger les faits et effectuer l'export final.
 */
class DatabasePipeline {
    public static void runAll(SparkSession spark) {
        MySqlWriter writer = getWriter();
        String geoFile = "src/main/resources/inputs/FichierReferenceCommunesDepartement/communes-france.csv";

        // 1) Chargement des dimensions (Commune + Departement)
        MySQLConverter.loadAndWriteCommunesDepartement(spark, writer, new File(geoFile));

        // 2) PK/FK sur les dimensions
        writer.execSql("SET FOREIGN_KEY_CHECKS=1");
        writer.addPrimaryKey("Departement", "code_dep");
        writer.addPrimaryKey("Commune",     "code_com");
        writer.addForeignKey(
            "Commune", "fk_commune_dept",
            "code_dep",
            "Departement", "code_dep",
            "RESTRICT", "CASCADE"
        );

        // 3) Chargement des faits (création de Donnees_commune)
        MySQLConverter.processOutputCSVs(spark, writer,      "src/main/resources/outputs");
        MySQLConverter.processDepartementsCSVsOnCommunes(spark, writer, "src/main/resources/outputs");
        MySQLConverter.processRegionsCSVsOnCommunes(spark,    writer, "src/main/resources/outputs");

        // 4) Adapter le type de CODGEO pour pouvoir créer la FK
        writer.modifyColumnType("Donnees_commune", "CODGEO", "VARCHAR(5) NOT NULL");

        // 5) Ajouter la FK sur CODGEO
        writer.execSql(
            "ALTER TABLE `Donnees_commune` " +
            "ADD CONSTRAINT `fk_data_commune` " +
            "FOREIGN KEY (`CODGEO`) REFERENCES `Commune`(`code_com`) " +
            "ON DELETE RESTRICT"
        );

        // 6) Adapter et ajouter la FK sur Code département
        writer.modifyColumnType("Donnees_commune", "Code département", "INT NOT NULL");
        writer.execSql(
            "ALTER TABLE `Donnees_commune` " +
            "ADD CONSTRAINT `fk_data_dept` " +
            "FOREIGN KEY (`Code département`) REFERENCES `Departement`(`code_dep`) " +
            "ON DELETE RESTRICT ON UPDATE CASCADE"
        );

        // 7) Export final des faits en CSV
        MySQLConverter.exportDonneesCommuneByYearAndType(
            spark,
            writer,
            "src/main/resources/exports/Donnees_commune"
        );
    }

	/**
	 * Renvoie une instance partagée de MySqlWriter pour la configuration de
	 * connexion.
	 */
	private static MySqlWriter getWriter() {
		return new MySqlWriter("localhost", // hôte
				"3306", // port
				"MSPRLegislative", // base
				"poix", // utilisateur
				"poix" // mot de passe
		);
	}
}
