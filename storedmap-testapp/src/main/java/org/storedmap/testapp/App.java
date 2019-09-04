/*
 * Copyright 2018 Fyodor Kravchenko {@literal(<fedd@vsetec.com>)}.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.storedmap.testapp;

import org.storedmap.Category;
import org.storedmap.MixedDriver;
import org.storedmap.Store;
import org.storedmap.StoredMap;
import org.storedmap.Util;
import org.storedmap.elasticsearch.ElasticsearchDriver;
import org.storedmap.jdbc.GenericJdbcDriver;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.codec.binary.Base32;

/**
 *
 * @author Fyodor Kravchenko {@literal(<fedd@vsetec.com>)}
 */
public class App {

    public static void main(String[] args) {

        Properties elasticsearch = new Properties();
        elasticsearch.setProperty("applicationCode", "testapp");
        elasticsearch.setProperty("driver", ElasticsearchDriver.class.getName());
        elasticsearch.setProperty("elasticsearch.host", "localhost");
        elasticsearch.setProperty("elasticsearch.port", "9200");

        Properties postgres = new Properties();
        postgres.setProperty("applicationCode", "testapp");
        postgres.setProperty("driver", GenericJdbcDriver.class.getName());
        postgres.setProperty("jdbc.driver", "org.postgresql.Driver");
        postgres.setProperty("jdbc.url", "jdbc:postgresql://localhost:5432/testapp04");
        postgres.setProperty("jdbc.user", "postgres");
        postgres.setProperty("jdbc.password", "postgres");
        postgres.setProperty("jdbc.queries.create",
                "create table @{indexName}_main (id varchar(200) primary key, val bytea);\n"
                + "create table @{indexName}_lock (id varchar(200) primary key, createdat timestamp, waitfor integer, session varchar(200));\n"
                + "create table @{indexName}_indx (id varchar(200), sec varchar(200), tag varchar(200), sort bytea, map text, primary key (tag, id));\n"
                + "create index @{indexName}_ind1 on @{indexName}_indx (sort, tag);\n"
                + "create index @{indexName}_ind2 on @{indexName}_indx (id);\n"
                + "create index @{indexName}_ind3 on @{indexName}_indx (sec, sort, tag)");

        Properties derby = new Properties();
        derby.setProperty("applicationCode", "testapp");
        derby.setProperty("driver", GenericJdbcDriver.class.getName());
        derby.setProperty("jdbc.driver", "org.apache.derby.jdbc.EmbeddedDriver");
        derby.setProperty("jdbc.url", "jdbc:derby:testapp;create=true");

        Properties mixed = new Properties();
        mixed.putAll(elasticsearch);
        mixed.putAll(postgres);
        mixed.setProperty("driver", MixedDriver.class.getName());
        mixed.setProperty("driver.main", GenericJdbcDriver.class.getName());
        mixed.setProperty("driver.additional", ElasticsearchDriver.class.getName());

        String[] categoryNames = new String[]{
           // "themap",
            //"aMap"
            "random"
        //"very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very LONG NAME",
        //"По-русски",
        //"_underscore"
        };

        Store store;

        store = new Store(elasticsearch);

        for (String cat : categoryNames) {

            for (int i = 0; i < 100; i++) {


                Category category = store.get(cat + (int)(Math.random()*4));



                //StoredMap map1 = category.map("map" + i);
                HashMap map = new HashMap();

                for (int j = 0; j < 2; j++) {
                    map.put("key" + j, "value" + j + " of map " + i + " in a category " + category.name());
                }

//                if (i == 3) {
//                    map.tags(new String[]{"odd", "third"});
//                } else if ((i & 1) != 0) {
//                    map.tags(new String[]{"odd"});
//                } else {
//                    map.secondaryKey("even");
//                }
//                //map.sorter(Instant.now());
//                //map.sorter(Integer.toString(i));
//                map.sorter(i);

                category.put("map" + i, map);
            }

        }

        System.out.println("\nCategories:");

        for (Category category : store.categories()) {
            System.out.println("\nMaps in category " + category.name() + ":");
            System.out.println("(internal name - " + category.internalIndexName() + ")");
            for (StoredMap map : category.maps()) {
                System.out.println("\nMap id:\t" + map.key());
                System.out.println("Map Key:\t" + map.secondaryKey() + ",\tMap Sorter:\t" + map.sorter() + ",\tMap Tags:\t" + Arrays.toString(map.tags()));
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    System.out.println("Key:\t" + entry.getKey() + "\tvalue:\t" + entry.getValue());
                }
            }
        }

        store.close();
        
//        
//        // restart the store to make sure all StoredMaps are persisted
//        store = Store.getStore(elasticsearch);
//
//        System.out.println("\n***************************\nTags test:");
//        for (Category category : store.categories()) {
//            long cnt = category.count(null, null, null, new String[]{"odd", "third"}, null);
//            System.out.println("\nMaps in category " + category.name() + " with odd and third tag - " + cnt + " items");
//            for (StoredMap map : category.maps(null, null, null, new String[]{"odd", "third"}, null, null)) {
//                System.out.println("\nMap id:\t" + map.key());
//                System.out.println("Sorter:\t" + map.sorter() + ",\tTags:\t" + Arrays.toString(map.tags()));
//                for (Map.Entry<String, Object> entry : map.entrySet()) {
//                    System.out.println("Key:\t" + entry.getKey() + "\tvalue:\t" + entry.getValue());
//                }
//            }
//        }
//
//        System.out.println("\n***************************\nSorting and filtering test:");
//        for (Category category : store.categories()) {
//            long cnt = category.count(null, 0, 2, null, null);
//            System.out.println("\nMaps in category " + category.name() + " from 0 to 2, ascending - " + cnt + " items");
//            for (StoredMap map : category.maps(null, 0, 2, null, true, null)) {
//                System.out.println("\nMap id:\t" + map.key());
//                System.out.println("Sorter:\t" + map.sorter() + ",\tTags:\t" + Arrays.toString(map.tags()));
//                for (Map.Entry<String, Object> entry : map.entrySet()) {
//                    System.out.println("Key:\t" + entry.getKey() + "\tvalue:\t" + entry.getValue());
//                }
//            }
//        }
//
//        System.out.println("\n***************************\nTags and filtering test:");
//        for (Category category : store.categories()) {
//            long cnt = category.count(null, 1, 2, new String[]{"odd", "third"}, null);
//            System.out.println("\nMaps in category " + category.name() + " with odd and third tag ordered and filtered - " + cnt + " items");
//            for (StoredMap map : category.maps(null, 1, 2, new String[]{"odd", "third"}, true, null)) {
//                System.out.println("\nMap id:\t" + map.key());
//                System.out.println("Sorter:\t" + map.sorter() + ",\tTags:\t" + Arrays.toString(map.tags()));
//                for (Map.Entry<String, Object> entry : map.entrySet()) {
//                    System.out.println("Key:\t" + entry.getKey() + "\tvalue:\t" + entry.getValue());
//                }
//            }
//        }
//
//        System.out.println("\n***************************\nSecondary Key test:");
//        for (Category category : store.categories()) {
//            long cnt = category.count("even", null, null, null, null);
//            System.out.println("\nMaps in category " + category.name() + " with even secondary key - " + cnt + " items");
//            for (StoredMap map : category.maps("even", null, null, null, null, null)) {
//                System.out.println("\nMap id:\t" + map.key());
//                System.out.println("Sorter:\t" + map.sorter() + ",\tTags:\t" + Arrays.toString(map.tags()));
//                for (Map.Entry<String, Object> entry : map.entrySet()) {
//                    System.out.println("Key:\t" + entry.getKey() + "\tvalue:\t" + entry.getValue());
//                }
//            }
//        }

//        store.close();

    }
}
