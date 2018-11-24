/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.vsetec.storedmap.testapp;

import com.vsetec.storedmap.Category;
import com.vsetec.storedmap.MixedDriver;
import com.vsetec.storedmap.Store;
import com.vsetec.storedmap.StoredMap;
import com.vsetec.storedmap.Util;
import com.vsetec.storedmap.elasticsearch.ElasticsearchDriver;
import com.vsetec.storedmap.jdbc.GenericJdbcDriver;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.codec.binary.Base32;

/**
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class App {

    public static void main(String[] args) {

//        Base32 b32 = new Base32(true);
//        
//        for(double i=-10.00; i<10.00; i=i+0.1){
//            System.out.println(i + ": \t" + b32.encodeAsString(Util.translateSorterIntoBytes(i, null, 32)));
//        }
        Properties elasticsearch = new Properties();
        elasticsearch.setProperty("storedmap.applicationCode", "testapp");
        elasticsearch.setProperty("storedmap.driver", ElasticsearchDriver.class.getName());
        elasticsearch.setProperty("storedmap.elasticsearch.host", "localhost");
        elasticsearch.setProperty("storedmap.elasticsearch.port", "9200");

        Properties postgres = new Properties();
        postgres.setProperty("storedmap.applicationCode", "testapp");
        postgres.setProperty("storedmap.driver", GenericJdbcDriver.class.getName());
        postgres.setProperty("storedmap.jdbc.driver", "org.postgresql.Driver");
        postgres.setProperty("storedmap.jdbc.url", "jdbc:postgresql://localhost:5432/testapp04");
        postgres.setProperty("storedmap.jdbc.user", "postgres");
        postgres.setProperty("storedmap.jdbc.password", "postgres");
        postgres.setProperty("storedmap.jdbc.queries.create",
                "create table @{indexName}_main (id varchar(200) primary key, val bytea);\n"
                + "create table @{indexName}_lock (id varchar(200) primary key, createdat timestamp, waitfor integer);\n"
                + "create table @{indexName}_indx (id varchar(200), tag varchar(200), sort bytea, map text, primary key (tag, id));\n"
                + "create index @{indexName}_ind1 on @{indexName}_indx (sort, tag);\n"
                + "create index @{indexName}_ind2 on @{indexName}_indx (id)");

        Properties derby = new Properties();
        derby.setProperty("storedmap.applicationCode", "testapp");
        derby.setProperty("storedmap.driver", GenericJdbcDriver.class.getName());
        derby.setProperty("storedmap.jdbc.driver", "org.apache.derby.jdbc.EmbeddedDriver");
        derby.setProperty("storedmap.jdbc.url", "jdbc:derby:testapp;create=true");

        Properties mixed = new Properties();
        mixed.putAll(elasticsearch);
        mixed.putAll(postgres);
        mixed.setProperty("storedmap.driver", MixedDriver.class.getName());
        mixed.setProperty("storedmap.driver.main", GenericJdbcDriver.class.getName());
        mixed.setProperty("storedmap.driver.additional", ElasticsearchDriver.class.getName());

        Store store = Store.getStore(elasticsearch);

        String[] categoryNames = new String[]{
            "themap",
            "aMap"
        //"very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very LONG NAME",
        //"По-русски",
        //"_underscore"
        };

        for (String cat : categoryNames) {

            Category category = store.get(cat);

            for (int i = 0; i < 4; i++) {
                StoredMap map = category.map("map" + i);

                for (int j = 0; j < 2; j++) {
                    map.put("key" + j, "value" + j + " of map " + i + " in a category " + category.name());
                }

                if (i == 3) {
                    map.tags(new String[]{"odd", "third"});
                } else if ((i & 1) != 0) {
                    map.tags(new String[]{"odd"});
                }
                map.sorter(Instant.now());
                //map.sorter(Integer.toString(i));
                //map.sorter(i);
            }

        }

        System.out.println("\nCategories:");

        for (Category category : store.categories()) {
            System.out.println("\nMaps in category " + category.name() + ":");
            System.out.println("(internal name - " + category.internalIndexName() + ")");
            for (StoredMap map : category.maps()) {
                System.out.println("\nMap id:\t" + map.key());
                System.out.println("Sorter:\t" + map.sorter() + ",\tTags:\t" + Arrays.toString(map.tags()));
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    System.out.println("Key:\t" + entry.getKey() + "\tvalue:\t" + entry.getValue());
                }
            }
        }

        store.close();

    }
}
