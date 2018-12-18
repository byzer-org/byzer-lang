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

package streaming.common.shell;

import java.security.MessageDigest;

/**
 * Created by allwefantasy on 12/7/2017.
 */
public class Md5 {
    public Md5() {
    }

    public static final String MD5(String s) {
        try {
            byte[] e = s.getBytes();
            MessageDigest mdInst = MessageDigest.getInstance("MD5");
            mdInst.update(e);
            byte[] md = mdInst.digest();
            StringBuffer sb = new StringBuffer();

            for(int i = 0; i < md.length; ++i) {
                int val = md[i] & 255;
                if(val < 16) {
                    sb.append("0");
                }

                sb.append(Integer.toHexString(val));
            }

            return sb.toString();
        } catch (Exception var7) {
            return null;
        }
    }

    public static void main(String[] args) {
        String err = "e,3,4,";
        System.out.println(err.substring(0, err.length() - 1));
    }
}
