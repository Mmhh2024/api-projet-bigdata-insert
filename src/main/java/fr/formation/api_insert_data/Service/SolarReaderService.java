package fr.formation.api_insert_data.Service;

//public class SolarReaderService {

//}

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import fr.formation.api_insert_data.dto.SolarWindDto;

@Service
public class SolarReaderService {
    private static final Logger log = LoggerFactory.getLogger(SolarReaderService.class);

    public List<SolarWindDto> read(String filename) {
        List<SolarWindDto> winds = new ArrayList<>();

        log.debug("Ouverture du fichier Discovery {} ...", filename);

        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            int index = 0;

            while ((line = br.readLine()) != null) {
                // En-tête : Date;Speed;Density;Bt;Bz
                if (index++ < 1) { // On est dans l'en-tête, donc on ignore
                    continue; // Boucler directement
                }

                String[] infos = line.split(";");
                SolarWindDto dto = new SolarWindDto();

                dto.setDate(infos[0]);

                if (!infos[1].isBlank()) {
                    dto.setSpeed(Float.parseFloat(infos[1]));
                }
                
                if (!infos[2].isBlank()) {
                    dto.setDensity(Float.parseFloat(infos[2]));
                }
                
                if (!infos[3].isBlank()) {
                    dto.setBt(Float.parseFloat(infos[3]));
                }
                
                if (!infos[4].isBlank()) {
                    dto.setBz(Float.parseFloat(infos[4]));
                }

                winds.add(dto);
            }

            log.debug("{} solar winds processed!", index);
        }

        catch (Exception ex) {
            log.error("Erreur pendant la lecture du fichier {}...", filename);
        }

        return winds;
    }
}
