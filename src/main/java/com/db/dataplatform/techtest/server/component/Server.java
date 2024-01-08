package com.db.dataplatform.techtest.server.component;

import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
//import org.springframework.retry.annotation.Retryable;
import org.springframework.retry.annotation.Retryable;
import org.springframework.web.client.HttpServerErrorException;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public interface Server {
    boolean saveDataEnvelope(DataEnvelope envelope, final String clientChecksum) throws IOException, NoSuchAlgorithmException;

    @Retryable(value = {HttpServerErrorException.GatewayTimeout.class},maxAttempts = 3)
    void pushDataToHadoop(DataEnvelope envelope);

    List<DataEnvelope> getDataEnvelopByBlockType(String blockType);

    boolean updateDataBlockType(String name, String newBlockType);
}
