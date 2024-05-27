package fr.formation.api_insert_data.service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import fr.formation.api_insert_data.dto.KirunaDto;

@Service
public class KirunaReaderService {
    private static final Logger log = LoggerFactory.getLogger(KirunaReaderService.class);

    public List<KirunaDto> read(String filename) {
        List<KirunaDto> kirunaData = new ArrayList<>();

        log.debug("Ouverture du fichier Kiruna {} ...", filename);

        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            int index = 0;

            while ((line = br.readLine()) != null) {
                String[] infos = line.split("\t");
                KirunaDto dto = new KirunaDto();

                dto.setDate(infos[0]);

                if (!infos[1].isBlank()) {
                    dto.setX(Float.parseFloat(infos[1]));
                }
                
                kirunaData.add(dto);
            }

            log.debug("{} kiruna data processed!", index);
        }

        catch (Exception ex) {
            log.error("Erreur pendant la lecture du fichier {}...", filename);
            ex.printStackTrace();
        }

        return kirunaData;
    }
}
