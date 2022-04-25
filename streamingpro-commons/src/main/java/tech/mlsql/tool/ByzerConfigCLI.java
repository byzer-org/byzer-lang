/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.mlsql.tool;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public class ByzerConfigCLI {

    private final static String SPARK_CONF_TEMP = "--conf %s=%s";

    private final static String BYZER_CONF_TEMP = "-%s %s";

    private final static String ARGS_CONF_TEMP = "-%s %s ";

    public static void main(String[] args) {
        execute(args);
        Unsafe.systemExit(0);
    }

    public static void execute(String[] args) {
        boolean needDec = false;
        if (args.length != 1) {
            if (args.length < 2 || !Objects.equals(EncryptUtil.DEC_FLAG, args[1])) {
                System.out.println("Usage: ByzerConfigCLI conf_name");
                System.out.println("Example: ByzerConfigCLI byzer.server.mode");
                Unsafe.systemExit(1);
            } else {
                needDec = true;
            }
        }

        Properties config = ByzerConfig.getInstance().getProperties();

        String key = args[0].trim();
        if (key.equals("-byzer")) {
            // get byzer properties
            for (Map.Entry<Object, Object> entry : config.entrySet()) {
                String entryKey = (String) entry.getKey();
                if (entryKey.startsWith("streaming") || entryKey.startsWith("spark.mlsql")) {
                    String prop = String.format(BYZER_CONF_TEMP, entryKey, entry.getValue());
                    System.out.println(prop);
                }
            }
        } else if (key.equals("-spark")) {
            // get spark properties
            for (Map.Entry<Object, Object> entry : config.entrySet()) {
                String entryKey = (String) entry.getKey();
                if (entryKey.startsWith("spark") && !entryKey.startsWith("spark.mlsql")) {
                    String prop = String.format(SPARK_CONF_TEMP, entryKey, entry.getValue());
                    System.out.println(prop);
                }
            }
        } else if ("-args".equals(key)) {
            // get all properties
            StringBuffer prop = new StringBuffer("");
            for (Map.Entry<Object, Object> entry : config.entrySet()) {
                String entryKey = (String) entry.getKey();
                prop.append(String.format(ARGS_CONF_TEMP, entryKey, entry.getValue()));
            }
            System.out.println(prop);
        }
        else if (!key.endsWith(".")) {
            String value = config.getProperty(key);
            if (value == null) {
                value = "";
            }
            if (needDec && EncryptUtil.isEncrypted(value)) {
                System.out.println(EncryptUtil.decryptPassInKylin(value));
            } else {
                System.out.println(value.trim());
            }
        } else {
            Map<String, String> props = getPropertiesByPrefix(config, key);
            for (Map.Entry<String, String> prop : props.entrySet()) {
                System.out.println(prop.getKey() + "=" + prop.getValue().trim());
            }
        }
    }

    private static Map<String, String> getPropertiesByPrefix(Properties props, String prefix) {
        Map<String, String> result = Maps.newLinkedHashMap();
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            String entryKey = (String) entry.getKey();
            if (entryKey.startsWith(prefix)) {
                result.put(entryKey.substring(prefix.length()), (String) entry.getValue());
            }
        }
        return result;
    }
}
