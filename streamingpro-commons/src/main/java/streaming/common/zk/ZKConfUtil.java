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

package streaming.common.zk;

/**
 * 7/7/16 WilliamZhu(allwefantasy@gmail.com)
 */
import net.csdn.ServiceFramwork;
import net.csdn.common.logging.CSLogger;
import net.csdn.common.logging.Loggers;
import net.csdn.common.settings.Settings;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;

import java.text.SimpleDateFormat;
import java.util.*;


public class ZKConfUtil {

    private final int MAX_SIZE = 5;
    public static String CONF_ROOT_DIR;
    public SerializableSerializer serial;
    private String serverList;
    private SimpleDateFormat fullFormatter = new SimpleDateFormat("yyyyMMdd_HH:mm:ss");
    public ZkClient client;
    private Settings settings;
    private CSLogger logger = Loggers.getLogger(ZKConfUtil.class);


    public static ZKConfUtil create(Settings settings) {
        ZKConfUtil zkConfUtil = new ZKConfUtil();
        zkConfUtil.settings = settings;
        zkConfUtil.CONF_ROOT_DIR = settings.get(ServiceFramwork.mode + ".zk.conf_root_dir", "/streamingpro/sparksql/service");
        zkConfUtil.serial = new SerializableSerializer();
        zkConfUtil.serverList = settings.get(ServiceFramwork.mode + ".zk.servers", "127.0.0.1:2181");
        zkConfUtil.client = new ZkClient(zkConfUtil.serverList);
        return zkConfUtil;
    }


    public String getConfPath(String confName) {

        return CONF_ROOT_DIR + "/" + confName;
    }


    public boolean ifExist(String path) {

        if (path == null || path.isEmpty()) {

            return false;
        }

        return client.exists(path);

    }

    public String getValue(String path) {

        if (path == null || path.isEmpty()) {

            return null;
        }
        return client.readData(path);
    }

    public TreeMap<String, String> getObjectValue(Object obj) {

        if (obj != null && obj instanceof byte[]) {
            return (TreeMap<String, String>) serial.deserialize((byte[]) obj);
        }
        return null;
    }

    public String getConf(Object obj) {


        if (obj != null) {

            TreeMap<String, String> map = getObjectValue(obj);

            if (map != null && map.size() > 0) {

                return map.lastEntry().getValue();
            }
        }

        return null;
    }

    public void checkAndAddPath(Path path) {


        String name = path.getName();
        if ("/".equals(name) || "\\".equals(name)) {

            return;
        } else {

            String p = path.getPathString().replaceAll("\\+", "/");
            p = p.replaceAll("/+$", "");
            if (ifExist(p)) {

                return;
            } else {

                checkAndAddPath(path.getParentPath());
                System.out.println("add path " + p);
                client.createPersistent(p);
            }
        }

    }

    public void addPath(String path, String value) {

        if (path == null) {


            return;
        }

        checkAndAddPath(new Path(path).getParentPath());
        if (value == null || value.isEmpty()) {

            client.createPersistent(path);
        } else {
            client.createPersistent(path, value);
        }

    }

    public void updatePath(String path, String value) {

        if (path == null || value == null || value.isEmpty()) {


            return;
        }
        client.writeData(path, value);

    }

    public byte[] addConf(Object obj, String value) {

        TreeMap<String, String> map = null;

        if (obj != null) {

            map = getObjectValue(obj);


        } else {

            map = new TreeMap<String, String>();
        }

        //
        //map.put(TimeUtil.getCurrentMinutes(), value);
        map.put(fullFormatter.format(new Date()), value);
        if (map.size() > MAX_SIZE) {
            map.remove(map.firstKey());
        }


        return serial.serialize(map);
    }


    public List<String[]> listConfValues(String confName) {

        Object data = client.readData(getConfPath(confName));


        if (data != null) {

            TreeMap<String, String> map = getObjectValue(data);

            List<String[]> list = new ArrayList<String[]>();
            for (Map.Entry<String, String> entry : map.entrySet()) {

                list.add(new String[]{entry.getKey(), entry.getValue()});
            }

            Collections.sort(list, new Comparator<String[]>() {

                @Override
                public int compare(String[] o1, String[] o2) {
                    // TODO Auto-generated method stub
                    return o2[0].compareTo(o1[0]);
                }
            });
            return list;
        }

        return null;

    }

    public void main(String[] args) throws InterruptedException {


        String dir = "/lock/127.0.0.1";

        if (!ifExist(dir)) {
            System.out.println("create dir");


            if (!ifExist("/lock")) {

                client.createPersistent("/lock");
            }

            client.createEphemeral(dir);
        }
        String path = client.createEphemeralSequential(dir, null);

        System.out.println("path " + path);

        for (String l : client.getChildren(dir)) {

            System.out.println(l);
        }

        Thread.sleep(50000);
        client.close();
    }
}
