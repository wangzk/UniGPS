package cn.edu.nju.pasalab.graph.impl.util;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class CSVUtils {
    public static class CSVSchema implements Serializable {
        public enum PropertyType {STRING, INT, DOUBLE};

        public Map<String, PropertyType> getColumnType() {
            return columnType;
        }


        public Map<String, Integer> getColumnIndex() {
            return columnIndex;
        }
        Map<String, PropertyType> columnType = new HashMap<>();
        Map<String, Integer> columnIndex = new HashMap<>();
        String columnNames[];

        public String[] getColumnNames() {
            return columnNames;
        }


        /**
         * Load the CSV Schema from HDFS file
         * @param csvSchemaFilePath
         * @throws IOException
         */
        public CSVSchema(Path csvSchemaFilePath) throws IOException {
            FileSystem fs = HDFSUtils.getFS(csvSchemaFilePath.toString());
            FSDataInputStream schemaStream = fs.open(csvSchemaFilePath);
            Scanner scanner = new Scanner(schemaStream);
            ArrayList<String> columnNames = new ArrayList<>();
            int index = 0;
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                String name = line.split(":")[0];
                String type = line.split(":")[1];
                System.out.println("Found new column: " + name + ", " + type);
                columnNames.add(name);
                columnIndex.put(name, index);
                if (type.equals("String")) {
                    columnType.put(name, PropertyType.STRING);
                } else if (type.equals("Double")) {
                    columnType.put(name, PropertyType.DOUBLE);
                } else if (type.equals("Integer")) {
                    columnType.put(name, PropertyType.INT);
                }
                index++;
            }
            this.columnNames = columnNames.toArray(new String[columnNames.size()]);
            scanner.close();
            schemaStream.close();
        }
        public CSVSchema(List<String> properties, Vertex testVertex) {
            columnNames = new String[properties.size()];
            for (int i = 0; i < properties.size(); i++) {
                String propertyName = properties.get(i);
                columnNames[i] = propertyName;
                columnIndex.put(propertyName, i);
                PropertyType type;
                Object value = testVertex.value(propertyName);
                if (value.getClass().equals(Double.class)) {
                    type = PropertyType.DOUBLE;
                } else if (value.getClass().equals(Integer.class)) {
                    type = PropertyType.INT;
                } else if (value.getClass().equals(String.class)) {
                    type = PropertyType.STRING;
                } else {
                    type = PropertyType.STRING;
                }
                columnType.put(propertyName, type);
            }
        }

        public String toSchemaDescription() {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < columnNames.length; i++) {
                builder.append(columnNames[i] + ":");
                switch (columnType.get(columnNames[i])) {
                    case DOUBLE:
                        builder.append("Double");
                        break;
                    case STRING:
                        builder.append("String");
                        break;
                    case INT:
                        builder.append("Integer");
                        break;
                }
                builder.append("\n");
            }
            return builder.toString();
        }

        public String toString() {
            return "CSV Schema:" + toSchemaDescription();
        }

        public Map<String,Object> parseCSVLine(String line) throws IOException {
            Map<String, Object> propertyMap = new HashMap<>();
            CSVRecord record = CSVParser.parse(line, CSVFormat.RFC4180).iterator().next();
            for (int i = 0; i < columnNames.length; i++) {
                String columnName = columnNames[i];
                PropertyType type = columnType.get(columnName);
                switch (type) {
                    case DOUBLE:
                        propertyMap.put(columnName, new Double(record.get(i)));
                        break;
                    case STRING:
                        propertyMap.put(columnName, record.get(i));
                        break;
                    case INT:
                        propertyMap.put(columnName, new Integer(record.get(i)));
                        break;
                }
            }
            return propertyMap;
        }
    }
}
