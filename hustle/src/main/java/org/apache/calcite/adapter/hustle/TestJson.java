//package org.apache.calcite.adapter.hustle;
//
//import org.json.simple.JSONArray;
//import org.json.simple.JSONObject;
//import org.json.simple.parser.JSONParser;
//import org.json.simple.parser.ParseException;
//
//import java.io.FileReader;
//import java.io.IOException;
//import java.io.Reader;
//import java.util.Iterator;
//
//
//import java.net.*;
//
//public class TestJson {
//    public static void main(String[] args){
//        JSONParser parser = new JSONParser();
//
//        try (Reader reader = new FileReader("/Users/chronis/code/hustle/catalog.json")) {
//            JSONObject jsonCatalog = (JSONObject) parser.parse(reader);
//
//            JSONArray jsonTables = (JSONArray) jsonCatalog.get("tables");
//            Iterator<JSONObject> tablesIterator = jsonTables.iterator();
//            while (tablesIterator.hasNext()) {
//                JSONObject jsonTable = tablesIterator.next();
//                String tableName = (String) jsonTable.get("name");
//                System.out.println(tableName);
//                JSONArray jsonColumns = (JSONArray) jsonTable.get("columns");
//                Iterator<JSONObject>  columnsIterator = jsonColumns.iterator();
//                while (columnsIterator.hasNext()) {
//                    JSONObject jsonColumn = columnsIterator.next();
//                    System.out.print(jsonColumn.get("name")+" - ");
//                    System.out.println(jsonColumn.get("column_type"));
//                }
//                System.out.println();
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
//
//    }
//}
//
