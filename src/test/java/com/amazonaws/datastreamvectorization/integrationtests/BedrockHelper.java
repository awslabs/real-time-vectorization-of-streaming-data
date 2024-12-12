package com.amazonaws.datastreamvectorization.integrationtests;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingModel;
import com.amazonaws.services.bedrock.AmazonBedrock;
import com.amazonaws.services.bedrock.AmazonBedrockClientBuilder;
import com.amazonaws.services.bedrock.model.GetFoundationModelRequest;
import com.amazonaws.services.bedrock.model.GetFoundationModelResult;
import com.amazonaws.services.bedrock.model.ValidationException;
import com.amazonaws.services.opensearch.AmazonOpenSearchClientBuilder;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import java.util.List;

@Slf4j
public class BedrockHelper {
    private static final List<EmbeddingModel> EMBEDDING_MODELS = List.of(
            EmbeddingModel.AMAZON_TITAN_TEXT_G1,
            EmbeddingModel.AMAZON_TITAN_TEXT_V2,
            EmbeddingModel.AMAZON_TITAN_MULTIMODAL_G1,
            EmbeddingModel.COHERE_EMBED_ENGLISH,
            EmbeddingModel.COHERE_EMBED_MULTILINGUAL
    );

    AmazonBedrock bedrockClient;

    public BedrockHelper() {
        String region = AmazonBedrockClientBuilder.standard().getRegion();

        bedrockClient = AmazonBedrockClientBuilder
                .standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("bedrock", region))
                .build();

    }

    public EmbeddingModel getSupportedEmbeddingModel() {
        for (EmbeddingModel model : EMBEDDING_MODELS) {
            try {
                GetFoundationModelRequest getFoundationModelRequest = new GetFoundationModelRequest()
                        .withModelIdentifier(model.getModelId());
                GetFoundationModelResult getFoundationModelResult = bedrockClient.getFoundationModel(getFoundationModelRequest);
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
