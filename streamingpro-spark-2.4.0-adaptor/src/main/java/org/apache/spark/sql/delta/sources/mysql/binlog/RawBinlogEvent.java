package org.apache.spark.sql.delta.sources.mysql.binlog;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;

public class RawBinlogEvent {

    private Event event;
    private String binlogFilename;
    private Long pos;

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    private String eventType;

    public Long getPos() {
        return pos;
    }

    public void setPos(Long pos) {
        this.pos = pos;
    }


    public RawBinlogEvent(Event event, String binlogFilename, String eventType, long pos) {
        this.event = event;
        this.binlogFilename = binlogFilename;
        this.pos = pos;
        this.eventType = eventType;
    }

    public Long getTimestamp() {
        EventHeaderV4 header = event.getHeader();
        return header.getTimestamp();
    }

    public Event getEvent() {
        return event;
    }

    public void setEvent(Event event) {
        this.event = event;
    }

    public String getBinlogFilename() {
        return binlogFilename;
    }
}
