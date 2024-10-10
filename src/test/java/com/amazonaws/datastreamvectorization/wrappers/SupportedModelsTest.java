package com.amazonaws.datastreamvectorization.wrappers;

import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingConfiguration;
import com.amazonaws.datastreamvectorization.embedding.model.EmbeddingModel;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static com.amazonaws.datastreamvectorization.embedding.model.EmbeddingModel.AMAZON_TITAN_MULTIMODAL_G1;
import static com.amazonaws.datastreamvectorization.embedding.model.EmbeddingModel.AMAZON_TITAN_TEXT_V2;

@RunWith(Parameterized.class)
public class SupportedModelsTest {

    // fields used together with @Parameter must be public
    @Parameter(0)
    public String modelId;
    @Parameter(1)
    public EmbeddingModel expectedModel;

    // creates the test data
    @Parameters(name = "{index}: Test with m1={0}, m2 ={1} ")
    public static Collection<Object[]> data() {
        Object[][] data = new Object[][]{
                {"amazon.titan-embed-text-v1", EmbeddingModel.AMAZON_TITAN_TEXT_G1},
                {"amazon.titan-embed-text-v2:0", AMAZON_TITAN_TEXT_V2},
                {"amazon.titan-embed-image-v1", AMAZON_TITAN_MULTIMODAL_G1},
                {"cohere.embed-english-v3", EmbeddingModel.COHERE_EMBED_ENGLISH},
                {"cohere.embed-multilingual-v3", EmbeddingModel.COHERE_EMBED_MULTILINGUAL}
        };
        return Arrays.asList(data);
    }

    @Test
    public void testSupportedModels() {
            EmbeddingConfiguration config = new EmbeddingConfiguration(modelId, Collections.EMPTY_MAP);
            Assert.assertEquals(expectedModel, config.getEmbeddingModel());
    }

}
