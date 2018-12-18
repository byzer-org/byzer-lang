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

import com.google.inject.Inject;
import net.csdn.common.settings.Settings;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.log4j.Logger;

/**
 * 7/7/16 WilliamZhu(allwefantasy@gmail.com)
 */
public class ZKClient {
    private Logger logger = Logger.getLogger(ZKClient.class);
    private ZKConfUtil zkConfUtil;

    @Inject
    public ZKClient(Settings settings) {
        this.zkConfUtil = ZKConfUtil.create(settings);
    }

    public ZKConfUtil zkConfUtil() {
        return zkConfUtil;
    }

    private void process(Object data, final ConfCallBack confCallBack, boolean needFormatData) {

        if (data != null && confCallBack != null) {
            confCallBack.setConf(needFormatData ? zkConfUtil.getConf(data) : data.toString());
        } else {
            confCallBack.setConf(null);
        }

    }


    public boolean addListenerByPathAndInit(String path, final ConfCallBack confCallBack) {

        return addListenerByPathAndInit(path, confCallBack, false);
    }


    public boolean addListenerByPathAndInit(String path, final ConfCallBack confCallBack, boolean needFormatData) {

        if (addListenerByPath(path, confCallBack, needFormatData)) {

            Object data = zkConfUtil.client.readData(path);
            process(data, confCallBack, needFormatData);

            return true;
        } else {

            return false;
        }


    }


    public boolean addListenerByPath(String path, final ConfCallBack confCallBack) {

        return addListenerByPath(path, confCallBack, false);
    }


    public boolean addListenerByPath(String path, final ConfCallBack confCallBack, final boolean needFormatData) {

        if (confCallBack == null || confCallBack == null) {

            return false;
        } else {

            zkConfUtil.client.subscribeDataChanges(path, new IZkDataListener() {

                @Override
                public void handleDataDeleted(String dataPath) throws Exception {

                    //process(data, confCallBack, needFormatData);

                }

                @Override
                public void handleDataChange(String dataPath, Object data) throws Exception {


                    process(data, confCallBack, needFormatData);


                }
            });

            return true;
        }
    }


    public boolean addListener(String confName, final ConfCallBack confCallBack, final boolean needFormatData) {


        return addListenerByPath(zkConfUtil.getConfPath(confName), confCallBack, needFormatData);

    }


    public boolean addListenerAndInit(String confName, final ConfCallBack confCallBack) {

        if (addListener(confName, confCallBack, true)) {

            Object data = zkConfUtil.client.readData(zkConfUtil.getConfPath(confName));
            if (data != null) {

                try {
                    confCallBack.setConf(zkConfUtil.getConf(data));

                } catch (Exception e) {

                    e.printStackTrace();
                }


            }
            return true;
        } else {
            return false;
        }

    }


    public Object getConf(String confName) {

        Object data = zkConfUtil.client.readData(zkConfUtil.getConfPath(confName));
        if (data != null) {
            return zkConfUtil.getConf(data);
        }
        return data;

    }


    public Object getValue(String path) {

        Object data = zkConfUtil.client.readData(path);

        return data;

    }


    public void main(String[] args) throws InterruptedException {


        addListenerAndInit("_recommend_ab_test", new ConfCallBack() {

            @Override
            public void setConf(String conf) {
                System.out.println(conf);
                logger.info("test" + conf);

            }
        });

        Thread.currentThread().sleep(1000000);
    }

    public interface ConfCallBack {
        public void setConf(String conf);
    }
}

