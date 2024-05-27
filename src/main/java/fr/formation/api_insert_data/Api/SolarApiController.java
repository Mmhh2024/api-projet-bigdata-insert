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

import fr.formation.api_insert_data.Service.SolarReaderService;
import fr.formation.api_insert_data.dto.SolarWindDto;

@RestController
@RequestMapping("/api/solar")
public class SolarApiController {
    private static final Logger log = LoggerFactory.getLogger(KirunaApiController.class);

    @Autowired
    private SolarReaderService readerService;

    @PostMapping
    public void readAndSave() {
        try (Connection connection = DriverManager.getConnection("jdbc:clickhouse://127.0.0.1:8123/satellite", "default", "")) {
            connection.setAutoCommit(false);

            //for (int i = 1; i <= 12; i++) {
                //this.readAndSave(connection, i);
                this.readAndSave(connection);
            //}
        }

        catch (Exception ex) {
            ex.printStackTrace();
            log.error("Impossible de se connecter ...");
        }
    }

    //private void readAndSave(Connection connection, int mois) {
        private void readAndSave(Connection connection) {
    
        //List<KirunaWindDto> winds = this.readerService.read("D:/Developpement/perso/data/solarwinds/dscovr/compiled/2017/2017" + String.format("%02d", mois) + ".csv");
        //List<KirunaWindDto> winds = this.readerService.read("D:/Developpement/perso/data/solarwinds/dscovr/compiled/2017/2017" + String.format("%02d", mois) + ".csv");
        //List<SolarWindDto> winds = this.readerService.read("D:/Developpement/perso/data/solarwinds/dscovr/compiled/2017/2017" + String.format("%02d", mois) + ".csv");
        List<SolarWindDto> winds = this.readerService.read("C:\\csharp\\API-BIGDATA\\api-insert-data\\dscvr.0000");

        // Récupérer un Statement pour exécuter la requête (un PreparedStatement est encore mieux !)
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO wind (date, speed, density, bt, bz) VALUES (?, ?, ?, ?, ?)")) {
            int batchIndex = 0;
            
            for (SolarWindDto wind : winds) {
                statement.setString(1, wind.getDate());
                statement.setFloat(2, wind.getSpeed() == null ? 0 : wind.getSpeed());
                statement.setFloat(3, wind.getDensity() == null ? 0 : wind.getDensity());
                statement.setFloat(4, wind.getBt() == null ? 0 : wind.getBt());
                statement.setFloat(5, wind.getBz() == null ? 0 : wind.getBz());

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
