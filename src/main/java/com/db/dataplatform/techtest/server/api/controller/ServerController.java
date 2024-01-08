package com.db.dataplatform.techtest.server.api.controller;

import com.db.dataplatform.techtest.server.api.model.DataBody;
import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.component.Server;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import javax.validation.ConstraintViolationException;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

@Slf4j
@Controller
@RequestMapping("/dataserver")
@RequiredArgsConstructor
@Validated
public class ServerController {

    private final Server server;


    @PostMapping(value = "/pushdata", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Boolean> pushData(@Valid @RequestBody DataEnvelope dataEnvelope, @RequestParam String clientChecksum) throws IOException, NoSuchAlgorithmException {


        log.info("Data envelope received: {}", dataEnvelope.getDataHeader().getName());
        boolean checksumPass = server.saveDataEnvelope(dataEnvelope,clientChecksum);

        log.info("Data envelope persisted. Attribute name: {}", dataEnvelope.getDataHeader().getName());

        return ResponseEntity.ok(checksumPass);
    }

    @GetMapping(value = "/data/{blockType}")
    public ResponseEntity<List<DataEnvelope>> getData(final @PathVariable String blockType) throws IOException{

        log.info("BlockType received: {}", blockType);
        try {
            List<DataEnvelope> dataEnvelopeList = server.getDataEnvelopByBlockType(blockType);
            log.info("Retrieved data {}",dataEnvelopeList);
            return ResponseEntity.ok(dataEnvelopeList);
        } catch (IllegalArgumentException e) {
            // Handle the case where an invalid blockType is provided
            log.error("Invalid BlockType: {}", blockType);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
        }
    }

    @PutMapping(value = "/update/{name}/{newBlockType}")
    public ResponseEntity<Boolean> updateBlockType(final @PathVariable @NotBlank String name, final @PathVariable String newBlockType){

        log.info("Updating block {} with blockType {}", name,newBlockType);

        try
        {
            boolean updated = server.updateDataBlockType(name, newBlockType);
            log.info("Data updated for {} with {} : {}", name, newBlockType, updated);
            return ResponseEntity.ok(updated);
        }catch (IllegalArgumentException e) {
            log.error("Invalid BlockType: {}", newBlockType);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();

        }
    }

}
