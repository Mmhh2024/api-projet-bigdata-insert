package fr.formation.api_insert_data.api;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import fr.formation.api_insert_data.response.EventsNumResponse;

@RestController
@RequestMapping("/api/event")
public class EventApiController {

    private static final Logger log = LoggerFactory.getLogger(EventApiController.class);


    @GetMapping("/{year}")
    public EventsNumResponse getNumberEvents(@PathVariable("year") int year){
        EventsNumResponse resp = new EventsNumResponse();
        int eventsNumber = 0;
        resp.setYear(year);

        try (Connection connection = DriverManager.getConnection("jdbc:clickhouse://127.0.0.1:8123/projet_solar",
                "default", "")) {
            connection.setAutoCommit(false);

            try (PreparedStatement statement = connection
                .prepareStatement("SELECT * FROM satellite WHERE Bz != -999.9 AND date LIKE ?")){
                    statement.setString(1,year+"%");
                    ResultSet rs = statement.executeQuery();
                    while(rs.next()){
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

                        LocalDateTime date = LocalDateTime.parse(rs.getString("date"),formatter);
                        LocalDateTime dateMax = date.plus(40,ChronoUnit.MINUTES);

                        PreparedStatement statementKiruna = connection.prepareStatement("SELECT COUNT(*) AS k_count FROM kiruna WHERE date BETWEEN ? AND ?");
                        statementKiruna.setString(1,date.format(formatter));
                        statementKiruna.setString(2,dateMax.format(formatter));

                        ResultSet rsKiruna = statementKiruna.executeQuery();
                        rsKiruna.next();
                        int matchKiruna = rsKiruna.getInt("k_count");
                        rsKiruna.close();
                        if(matchKiruna > 0){
                            eventsNumber++;
                        }
                    }
                }
            catch(Exception ex){
                log.error("Problème avec la requête ...");
            }

        }

        catch (Exception ex) {
            log.error("Impossible de se connecter ...");
        }
        resp.setNumberEvents(eventsNumber);
        return resp;
    }
    
}
