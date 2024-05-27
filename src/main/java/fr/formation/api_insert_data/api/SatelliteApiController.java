package fr.formation.api_insert_data.api;

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

import fr.formation.api_insert_data.dto.SatelliteDto;
import fr.formation.api_insert_data.service.SatelliteReaderService;

@RestController
@RequestMapping("/api/sat")
public class SatelliteApiController {
    private static final Logger log = LoggerFactory.getLogger(SatelliteApiController.class);

    @Autowired
    private SatelliteReaderService readerService;

    @PostMapping
    public void readAndSave() {
        try (Connection connection = DriverManager.getConnection("jdbc:clickhouse://127.0.0.1:8123/projet_solar",
                "default", "")) {
            connection.setAutoCommit(false);

            this.readAndSave(connection);

        }

        catch (Exception ex) {
            ex.printStackTrace();
            log.error("Impossible de se connecter ...");
        }
    }

    private void readAndSave(Connection connection) {
     
        List<SatelliteDto> satData = this.readerService.read("/home/pauline/eclipse-workspace/api-projet-bigdata-insert/sat_traite_gp2/part-00000");

        try (PreparedStatement statement = connection
                .prepareStatement("INSERT INTO satellite (date, Bz) VALUES (?, ?)")) {
            int batchIndex = 0;

            for (SatelliteDto sat : satData) {
                statement.setString(1, sat.getDate());
                statement.setFloat(2, sat.getBz() == null ? 0 : sat.getBz());

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
