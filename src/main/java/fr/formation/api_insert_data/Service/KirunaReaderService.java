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

import fr.formation.api_insert_data.dto.KirunaWindDto;

@Service
public class KirunaReaderService {
    private static final Logger log = LoggerFactory.getLogger(KirunaReaderService.class);

    public List<KirunaWindDto> read(String filename) {
        List<KirunaWindDto> winds = new ArrayList<>();

        log.debug("Ouverture du fichier Kiruna {} ...", filename);

        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            int index = 0;

            while ((line = br.readLine()) != null) {
                // En-tête : Date;Speed;Density;Bt;Bz
                if (index++ < 1) { // On est dans l'en-tête, donc on ignore
                    continue; // Boucler directement
                }

                String[] infos = line.split(";");
                KirunaWindDto dto = new KirunaWindDto();

                dto.setDate(infos[0]);

                if (!infos[1].isBlank()) {
                    dto.setX(Float.parseFloat(infos[1]));
                }
                
                

                winds.add(dto);
            }

            log.debug("{} kiruna  winds processed!", index);
        }

        catch (Exception ex) {
            log.error("Erreur pendant la lecture du fichier {}...", filename);
        }

        return winds;
    }
}
