package com.amazonaws.datastreamvectorization.integrationtests;

import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingModel;
import com.amazonaws.services.bedrock.AmazonBedrock;
import com.amazonaws.services.bedrock.AmazonBedrockClientBuilder;
import com.amazonaws.services.bedrock.model.GetFoundationModelRequest;
import com.amazonaws.services.bedrock.model.GetFoundationModelResult;
import com.amazonaws.services.bedrock.model.ValidationException;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * Helper class to interact with Amazon Bedrock
 */
@Slf4j
public class BedrockHelper {
    private static final List<EmbeddingModel> EMBEDDING_MODELS = List.of(
            EmbeddingModel.AMAZON_TITAN_TEXT_G1,
            EmbeddingModel.AMAZON_TITAN_TEXT_V2,
            EmbeddingModel.AMAZON_TITAN_MULTIMODAL_G1,
            EmbeddingModel.COHERE_EMBED_ENGLISH,
            EmbeddingModel.COHERE_EMBED_MULTILINGUAL
    );
    private final AmazonBedrock bedrockClient;

    public BedrockHelper() {
        bedrockClient = AmazonBedrockClientBuilder.defaultClient();
    }

    /**
     * Finds a Bedrock model supported by the real time vectorization of streaming data application
     * that is available in the current AWS region. Will return the first model found that is available.
     * Throws exception if no supported models are available in the region.
     * @return EmbeddingModel available in the region
     */
    public EmbeddingModel getSupportedEmbeddingModel() {
        for (EmbeddingModel model : EMBEDDING_MODELS) {
            try {
                GetFoundationModelRequest getFoundationModelRequest = new GetFoundationModelRequest()
                        .withModelIdentifier(model.getModelId());
                GetFoundationModelResult getFoundationModelResult = bedrockClient
                        .getFoundationModel(getFoundationModelRequest);
                if (model.getModelId().equals(getFoundationModelResult.getModelDetails().getModelId())) {
                    return model;
                }
            } catch (ValidationException e) {
                log.info("Bedrock foundation model {} is not supported in the test region. Checking other models...",
                        model.getModelId());
            }
        }
        throw new RuntimeException("None of the supported Bedrock foundation models are available in the test region");
    }

}
