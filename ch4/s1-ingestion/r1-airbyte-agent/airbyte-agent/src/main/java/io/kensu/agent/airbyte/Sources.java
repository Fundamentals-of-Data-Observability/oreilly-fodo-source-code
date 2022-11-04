package io.kensu.agent.airbyte;

import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

import io.airbyte.config.StandardSyncInput;

import io.kensu.dam.*;
import io.kensu.dam.model.*;

public interface Sources {
    public static class AirbyteSourceFile implements KensuProcessor {
        private static final Logger LOGGER = LoggerFactory.getLogger(KensuAgent.class);

        @Override
        public boolean matches(String imageName) {
            return imageName != null && imageName.startsWith("airbyte/source-file");
        }

        @Override
        public void process(StandardSyncInput syncInput, KensuAgent agent) {
            // configured source for the sync
            JsonNode sourceConfiguration = syncInput.getSourceConfiguration();
            // "https://www.donneesquebec.ca/recherche/fr/dataset/857d007a-f195-434b-bc00-7012a6244a90/resource/16f55019-f05d-4375-a064-b75bce60543d/download/pf-mun-2019-2019.csv"
            String url = sourceConfiguration.get("url").textValue();
            // "csv"
            String format = sourceConfiguration.get("format").textValue();
            // "HTTPS"
            String providerStorage = sourceConfiguration.get("provider").get("storage").textValue();
            // "donneesquebec"
            String datasetName = sourceConfiguration.get("dataset_name").textValue();

            agent.sourceDS = new DataSource()
                    .name(datasetName)
                    .format(format)
                    .pk(new DataSourcePK()
                            .location(url)
                            .physicalLocationRef(agent.UNKNOWN_PL_REF));

            // validator => get schema => configured
            if (agent.recordSchemaValidatorStreams != null) {
                SchemaPK pk = new SchemaPK().dataSourceRef(new DataSourceRef().byPK(agent.sourceDS.getPk()));
                JsonNode sourceJsonSchema = agent.recordSchemaValidatorStreams.get(datasetName);
                // properties: {"field1": {"type": ["string", "null"]}, ...}
                // properties.fields: Iterator<Map.Entry<String,JsonNode>>
                Iterator<Map.Entry<String, JsonNode>> itFields = sourceJsonSchema.get("properties").fields();
                while (itFields.hasNext()) {
                    Map.Entry<String, JsonNode> field = itFields.next();
                    String fieldName = field.getKey();
                    String fieldType = field.getValue().get("type").elements().next().textValue();
                    // TODO better typing => we get `number` and `string`
                    Boolean fieldNullable = true; // TODO... no idea?
                    pk = pk.addFieldsItem(new FieldDef().name(fieldName).fieldType(fieldType).nullable(fieldNullable));
                }
                agent.sourceSC = new Schema().name(datasetName).pk(pk);
            } else {
                LOGGER.warn("No schema validator available, so no schema available for source: " + agent.source);
            }            
        }

    }
}
