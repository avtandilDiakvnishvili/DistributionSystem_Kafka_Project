package org.example;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class InvalidDataChecker implements FlatMapFunction<Row, String> {
    @Override
    public Iterator<String> call(Row row) throws Exception {
        List<String> invalidDataMessages = new ArrayList<>();

        String ts = row.getString(row.fieldIndex("ts"));
        if (ts.equals("1970-01-01 00:00:00")) {
            invalidDataMessages.add("Invalid timestamp: ts is '1970-01-01 00:00:00'");
        }

        for (int i = 0; i <= 3; i++) {
            Object sensorValue = row.get(row.fieldIndex("sensor" + i));
            if (sensorValue == null) {
                invalidDataMessages.add("Invalid sensor value: sensor" + i + " is null");
            }
        }

        return invalidDataMessages.iterator();
    }
}