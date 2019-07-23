package org.apache.spark.sql.delta.sources.mysql.binlog.io;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.spark.sql.delta.sources.mysql.binlog.RawBinlogEvent;

import java.io.IOException;
import java.io.StringWriter;

public abstract class AbstractEventWriter {

    private final JsonFactory JSON_FACTORY = new JsonFactory();
    protected JsonGenerator jsonGenerator;

    // Common method to create a JSON generator and start the root object. Should be called by sub-classes unless they need their own generator and such.
    protected void startJson(StringWriter outputStream, RawBinlogEvent event) throws IOException {
        jsonGenerator = createJsonGenerator(outputStream);
        jsonGenerator.writeStartObject();
        String eventType = event.getEventType();
        if (eventType == null) {
            jsonGenerator.writeNullField("type");
        } else {
            jsonGenerator.writeStringField("type", eventType);
        }
        Long timestamp = event.getTimestamp();
        if (timestamp == null) {
            jsonGenerator.writeNullField("timestamp");
        } else {
            jsonGenerator.writeNumberField("timestamp", event.getTimestamp());
        }

        if (event.getTableInfo() != null) {
            String db = event.getTableInfo().getDatabaseName();
            String tableName = event.getTableInfo().getTableName();
            String schema = event.getTableInfo().getSchema();

            jsonGenerator.writeStringField("databaseName", db);
            jsonGenerator.writeStringField("tableName", tableName);
            jsonGenerator.writeStringField("schema", schema);
        } else {
            jsonGenerator.writeNullField("databaseName");
            jsonGenerator.writeNullField("tableName");
            jsonGenerator.writeNullField("schema");
        }


    }

    protected void endJson() throws IOException {
        if (jsonGenerator == null) {
            throw new IOException("endJson called without a JsonGenerator");
        }
        jsonGenerator.writeEndObject();
        jsonGenerator.flush();
        jsonGenerator.close();
    }

    private JsonGenerator createJsonGenerator(StringWriter out) throws IOException {
        return JSON_FACTORY.createGenerator(out);
    }

    public abstract java.util.List<String> writeEvent(RawBinlogEvent event);
}
