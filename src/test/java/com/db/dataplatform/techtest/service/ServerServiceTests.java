package com.db.dataplatform.techtest.service;

import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.mapper.ServerMapperConfiguration;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import com.db.dataplatform.techtest.server.persistence.model.DataHeaderEntity;
import com.db.dataplatform.techtest.server.service.DataBodyService;
import com.db.dataplatform.techtest.server.component.Server;
import com.db.dataplatform.techtest.server.component.impl.ServerImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.modelmapper.ModelMapper;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.db.dataplatform.techtest.TestDataHelper.DUMMY_CHECKSUM;
import static com.db.dataplatform.techtest.TestDataHelper.createTestDataEnvelopeApiObject;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ServerServiceTests {

    @Mock
    private DataBodyService dataBodyServiceImplMock;

    @Mock
    private RestTemplate restTemplateMock;

    private ModelMapper modelMapper;

    private DataBodyEntity expectedDataBodyEntity;
    private DataEnvelope testDataEnvelope;

    private Server server;

    @Before
    public void setup() {
        ServerMapperConfiguration serverMapperConfiguration = new ServerMapperConfiguration();
        modelMapper = serverMapperConfiguration.createModelMapperBean();

        testDataEnvelope = createTestDataEnvelopeApiObject();
        expectedDataBodyEntity = modelMapper.map(testDataEnvelope.getDataBody(), DataBodyEntity.class);
        expectedDataBodyEntity.setDataHeaderEntity(modelMapper.map(testDataEnvelope.getDataHeader(), DataHeaderEntity.class));

        server = new ServerImpl(dataBodyServiceImplMock, modelMapper,restTemplateMock);
    }

    @Test
    public void shouldSaveDataEnvelopeAsExpected() throws NoSuchAlgorithmException, IOException {
        boolean success = server.saveDataEnvelope(testDataEnvelope, DUMMY_CHECKSUM);

        assertThat(success).isTrue();
        verify(dataBodyServiceImplMock, times(1)).saveDataBody(any(DataBodyEntity.class));
    }

    @Test
    public void shouldGetDataEnvelopAsExpected(){
        when(dataBodyServiceImplMock.getDataByBlockType(any(BlockTypeEnum.class)))
                .thenReturn(Collections.singletonList(expectedDataBodyEntity));

        List<DataEnvelope> response = server.getDataEnvelopByBlockType(BlockTypeEnum.BLOCKTYPEA.name());

        assertThat(response).hasSize(1);

    }

    @Test
    public void shouldUpdateDataEnvelopAsExpected(){
        when(dataBodyServiceImplMock.getDataByBlockName("TEST2"))
                .thenReturn(Optional.of(expectedDataBodyEntity));

        boolean response = server.updateDataBlockType("TEST2",BlockTypeEnum.BLOCKTYPEB.name());

        assertThat(response).isTrue();

    }

    @Test
    public void shouldNotUpdateDataEnvelopAsExpected(){
        when(dataBodyServiceImplMock.getDataByBlockName("TEST2"))
                .thenReturn(Optional.empty());

        boolean response = server.updateDataBlockType("TEST2",BlockTypeEnum.BLOCKTYPEB.name());

        assertThat(response).isFalse();

    }
}
