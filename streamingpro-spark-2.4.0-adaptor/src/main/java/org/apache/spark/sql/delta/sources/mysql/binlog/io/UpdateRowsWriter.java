package org.apache.spark.sql.delta.sources.mysql.binlog.io;


import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import org.apache.spark.sql.delta.sources.mysql.binlog.MySQLCDCUtils;
import org.apache.spark.sql.delta.sources.mysql.binlog.RawBinlogEvent;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

public class UpdateRowsWriter extends AbstractEventWriter {


    @Override
    public List<String> writeEvent(RawBinlogEvent event) {
        UpdateRowsEventData data = event.getEvent().getData();
        List<String> items = new ArrayList<>();

        for (Map.Entry<Serializable[], Serializable[]> row : data.getRows()) {
            try {
                StringWriter writer = new StringWriter();
                startJson(writer, event);
                final BitSet bitSet = data.getIncludedColumns();
                writeRow(row, bitSet);
                endJson();
                items.add(writer.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return items;
    }

    protected void writeRow(Map.Entry<Serializable[], Serializable[]> row, BitSet includedColumns) throws IOException {

        jsonGenerator.writeArrayFieldStart("columns");
        int i = includedColumns.nextSetBit(0);
        while (i != -1) {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeNumberField("id", i + 1);
            ColumnDefinition columnDefinition = event.getColumnByIndex(i);
            Integer columnType = null;
            if (columnDefinition != null) {
                jsonGenerator.writeStringField("name", columnDefinition.getName());

            }
            Serializable[] oldRow = row.getKey();
            Serializable[] newRow = row.getValue();

            if (oldRow[i] == null) {
                jsonGenerator.writeNullField("last_value");
            } else {
                jsonGenerator.writeObjectField("last_value", MySQLCDCUtils.getWritableObject(columnType, oldRow[i]));
            }

            if (newRow[i] == null) {
                jsonGenerator.writeNullField("value");
            } else {
                jsonGenerator.writeObjectField("value", MySQLCDCUtils.getWritableObject(columnType, newRow[i]));
            }
            jsonGenerator.writeEndObject();
            i = includedColumns.nextSetBit(i + 1);
        }
        jsonGenerator.writeEndArray();
    }
}
