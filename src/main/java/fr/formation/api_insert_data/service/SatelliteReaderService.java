package fr.formation.api_insert_data.service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import fr.formation.api_insert_data.dto.SatelliteDto;


@Service
public class SatelliteReaderService {
    private static final Logger log = LoggerFactory.getLogger(SatelliteReaderService.class);

    public List<SatelliteDto> read(String filename) {
        List<SatelliteDto> satData = new ArrayList<>();

        log.debug("Ouverture du fichier Satellite {} ...", filename);

        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            int index = 0;

            while ((line = br.readLine()) != null) {
                
                String[] infos = line.split("\t");
                SatelliteDto dto = new SatelliteDto();

                dto.setDate(infos[0]);

                if (!infos[1].isBlank()) {
                    dto.setBz((Float.parseFloat(infos[1])));
                }
                                
                satData.add(dto);
            }

            log.debug("{} satellite data processed!", index);
        }

        catch (Exception ex) {
            log.error("Erreur pendant la lecture du fichier {}...", filename);
            ex.printStackTrace();
        }

        return satData;
    }
}
