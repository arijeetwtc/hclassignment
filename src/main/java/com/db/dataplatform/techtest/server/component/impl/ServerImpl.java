package com.db.dataplatform.techtest.server.component.impl;

import com.db.dataplatform.techtest.server.api.model.DataBody;
import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.api.model.DataHeader;
import com.db.dataplatform.techtest.server.component.Server;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import com.db.dataplatform.techtest.server.persistence.model.DataHeaderEntity;
import com.db.dataplatform.techtest.server.service.DataBodyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ServerImpl implements Server {

    private static final String HADOOP_POST_URI =   "http://localhost:8090/hadoopserver/pushbigdata";
    private final DataBodyService dataBodyServiceImpl;
    private final ModelMapper modelMapper;

    private final RestTemplate restTemplate;

    /**
     * @param envelope
     * @param clientChecksum
     * @return true if there is a match with the client provided checksum.
     */
    @Override
    public boolean saveDataEnvelope(DataEnvelope envelope, final String clientChecksum) {

        String calculatedChecksum = calculateMD5Checksum(envelope.getDataBody());
        if(calculatedChecksum.equals(clientChecksum)){
            envelope.setMd5Checksum(clientChecksum);
        }


        // Save to persistence.
        persist(envelope);

        log.info("Data persisted successfully, data name: {}", envelope.getDataHeader().getName());

        pushDataToHadoop(envelope);
        return clientChecksum.equals(calculatedChecksum);
    }

    @Override
    @Retryable(value = {HttpServerErrorException.GatewayTimeout.class},maxAttempts = 3)
    public void pushDataToHadoop(final DataEnvelope envelope)
    {
        log.info("Pushing the data to Hadoop, payload : {}", envelope);
        restTemplate.postForEntity(HADOOP_POST_URI, envelope, Boolean.class);

    }
    @Recover
    private void recovery(Exception exception, String message){
        log.info("Pushing the data to Hadoop unsuccessful : {}", message);
        log.error(exception.getMessage(),exception);
    }

    @Override
    public List<DataEnvelope> getDataEnvelopByBlockType(final String blockType)
    {
        List<DataBodyEntity> dataBodyEntityList = dataBodyServiceImpl.getDataByBlockType(BlockTypeEnum.valueOf(blockType));
        return dataBodyEntityList.stream().map(this::dataEnvelopConverter).collect(Collectors.toList());
    }

    @Override
    public boolean updateDataBlockType(final String name, final String newBlockType)
    {
        Optional<DataBodyEntity> optionalDataBodyEntity = dataBodyServiceImpl.getDataByBlockName(name);

        if(optionalDataBodyEntity.isPresent()){
            DataBodyEntity dataBodyEntity = optionalDataBodyEntity.get();
                dataBodyEntity.getDataHeaderEntity().setBlocktype(BlockTypeEnum.valueOf(newBlockType));
                saveData(dataBodyEntity);
                return true;
        }
        return false;
    }

    private DataEnvelope dataEnvelopConverter(final DataBodyEntity dataBodyEntity)
    {
        DataHeader header = new DataHeader(dataBodyEntity.getDataHeaderEntity().getName(),dataBodyEntity.getDataHeaderEntity().getBlocktype());
        DataBody dataBody = new DataBody(dataBodyEntity.getDataBody());
        return new DataEnvelope(header,dataBody,dataBodyEntity.getChecksum());
    }

    private void persist(DataEnvelope envelope) {
        log.info("Persisting data with attribute name: {}", envelope.getDataHeader().getName());
        DataHeaderEntity dataHeaderEntity = modelMapper.map(envelope.getDataHeader(), DataHeaderEntity.class);

        DataBodyEntity dataBodyEntity = modelMapper.map(envelope.getDataBody(), DataBodyEntity.class);
        dataBodyEntity.setDataHeaderEntity(dataHeaderEntity);

        saveData(dataBodyEntity);
    }

    private void saveData(DataBodyEntity dataBodyEntity) {
        dataBodyServiceImpl.saveDataBody(dataBodyEntity);
    }

    private String calculateMD5Checksum(final DataBody dataBody) {
        try{
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(dataBody.getDataBody().getBytes());
            byte[] digest = md.digest();

            StringBuilder result = new StringBuilder();
            for (byte b : digest){
                result.append(String.format("%02x",b));
            }
            return result.toString();

        } catch (NoSuchAlgorithmException e)
        {
            throw new RuntimeException("Error calculating MD5 checksum", e);
        }
    }
}
