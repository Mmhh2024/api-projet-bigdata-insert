package fr.formation.api_insert_data.Api;

//public class SolarApiController {

//}

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import fr.formation.api_insert_data.Service.KirunaReaderService;
import fr.formation.api_insert_data.dto.KirunaWindDto;

@RestController
@RequestMapping("/api/kiruna")
public class KirunaApiController {
    private static final Logger log = LoggerFactory.getLogger(KirunaApiController.class);

    @Autowired
    private KirunaReaderService readerService;

    @PostMapping
    public void readAndSave() {
        try (Connection connection = DriverManager.getConnection("jdbc:clickhouse://127.0.0.1:8123/satellite", "default", "")) {
            connection.setAutoCommit(false);

            //for (int i = 1; i <= 12; i++) {
                //this.readAndSave(connection, i)
                this.readAndSave(connection);;
            //}
        }

        catch (Exception ex) {
            ex.printStackTrace();
            log.error("Impossible de se connecter ...");
        }
    }

    //private void readAndSave(Connection connection, int mois) {
        private void readAndSave(Connection connection) {
        List<KirunaWindDto> winds = this.readerService.read("C:\\csharp\\API-BIGDATA\\api-insert-data\\kiruna_traite_gp2\\part-00000"); 
        //+ String.format("%02d", mois) + ".csv");

        // Récupérer un Statement pour exécuter la requête (un PreparedStatement est encore mieux !)
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO wind (date, X) VALUES (?, ?)")) {
            int batchIndex = 0;
            
            for (KirunaWindDto wind : winds) {
                statement.setString(1, wind.getDate());
                statement.setFloat(5, wind.getX() == null ? 0 : wind.getX());

                statement.addBatch();

                if (batchIndex == 100_000) {
                    statement.executeBatch();
                    connection.commit();
                    batchIndex = -1;
                }

                batchIndex++;
            }

            statement.executeBatch();
            connection.commit();
        }

        catch (Exception ex) {
            ex.printStackTrace();
            log.error("Problème dans la requête ...");
        }
    }
}
