/*
 * Copyright 2018 Fyodor Kravchenko <fedd@vsetec.com>.
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
                + "create table @{indexName}_lock (id varchar(200) primary key, createdat timestamp, waitfor integer);\n"
                + "create table @{indexName}_indx (id varchar(200), tag varchar(200), sort bytea, map text, primary key (tag, id));\n"
                + "create index @{indexName}_ind1 on @{indexName}_indx (sort, tag);\n"
                + "create index @{indexName}_ind2 on @{indexName}_indx (id)");

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
            "themap",
            "aMap"
        //"very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very very Very LONG NAME",
        //"По-русски",
        //"_underscore"
        };

        Store store;

        store = Store.getStore(elasticsearch);

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
                //map.sorter(Instant.now());
                //map.sorter(Integer.toString(i));
                map.sorter(i);
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
        // restart the store to make sure all StoredMaps are persisted
        store = Store.getStore(elasticsearch);

        System.out.println("\n***************************\nTags test:");
        for (Category category : store.categories()) {
            long cnt = category.count(new String[]{"odd", "third"});
            System.out.println("\nMaps in category " + category.name() + " with odd and third tag - " + cnt + " items");
            for (StoredMap map : category.maps(new String[]{"odd", "third"})) {
                System.out.println("\nMap id:\t" + map.key());
                System.out.println("Sorter:\t" + map.sorter() + ",\tTags:\t" + Arrays.toString(map.tags()));
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    System.out.println("Key:\t" + entry.getKey() + "\tvalue:\t" + entry.getValue());
                }
            }
        }

        System.out.println("\n***************************\nSorting and filtering test:");
        for (Category category : store.categories()) {
            long cnt = category.count(0, 2);
            System.out.println("\nMaps in category " + category.name() + " from 0 to 2, ascending - " + cnt + " items");
            for (StoredMap map : category.maps(0, 2, true)) {
                System.out.println("\nMap id:\t" + map.key());
                System.out.println("Sorter:\t" + map.sorter() + ",\tTags:\t" + Arrays.toString(map.tags()));
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    System.out.println("Key:\t" + entry.getKey() + "\tvalue:\t" + entry.getValue());
                }
            }
        }

        System.out.println("\n***************************\nTags and filtering test:");
        for (Category category : store.categories()) {
            long cnt = category.count(1, 2, new String[]{"odd", "third"});
            System.out.println("\nMaps in category " + category.name() + " with odd and third tag ordered and filtered - " + cnt + " items");
            for (StoredMap map : category.maps(1, 2, new String[]{"odd", "third"}, true)) {
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
