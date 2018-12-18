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

package streaming.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.kamranzafar.jtar.TarOutputStream;

import javax.servlet.http.HttpServletResponse;
import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by allwefantasy on 11/7/2017.
 */
public class DownloadRunner {

    private static Logger logger = Logger.getLogger(DownloadRunner.class);

    public static int getTarFileByPath(HttpServletResponse res, String pathStr) {
        String[] paths = pathStr.split(",");
        try {
            OutputStream outputStream = res.getOutputStream();

            TarOutputStream tarOutputStream = new TarOutputStream(new BufferedOutputStream(outputStream));

            FileSystem fs = FileSystem.get(new Configuration());
            List<FileStatus> files = new ArrayList<FileStatus>();

            for (String path : paths) {
                Path p = new Path(path);
                if (fs.exists(p)) {
                    if (fs.isFile(p)) {
                        files.add(fs.getFileStatus(p));
                    } else if (fs.isDirectory(p)) {
                        FileStatus[] fileStatusArr = fs.listStatus(p);
                        if (fileStatusArr != null && fileStatusArr.length > 0) {

                            for (FileStatus cur : fileStatusArr) {
                                if (cur.isFile()) {
                                    files.add(cur);
                                }
                            }
                        }
                    }
                }

            }

            if (files.size() > 0) {
                FSDataInputStream inputStream = null;
                int len = files.size();
                int i = 1;
                for (FileStatus cur : files) {
                    logger.info("[" + i++ + "/" + len + "]" + ",读取文件" + cur);
                    inputStream = fs.open(cur.getPath());

                    tarOutputStream.putNextEntry(new HDFSTarEntry(cur, cur.getPath().getName()));
                    org.apache.commons.io.IOUtils.copyLarge(inputStream, tarOutputStream);
                    inputStream.close();

                }
                tarOutputStream.flush();
                tarOutputStream.close();
                return 200;
            } else return 400;

        } catch (Exception e) {
            e.printStackTrace();
            return 500;

        }
    }

    public static int getRawFileByPath(HttpServletResponse res, String path, long position) {

        try {
            FileSystem fs = FileSystem.get(new Configuration());

            Path p = new Path(path);
            if (fs.exists(p)) {

                List<FileStatus> files = new ArrayList<FileStatus>();

                //找出所有文件
                if (fs.isFile(p)) {
                    files.add(fs.getFileStatus(p));
                } else if (fs.isDirectory(p)) {

                    FileStatus[] fileStatusArr = fs.listStatus(p);
                    if (fileStatusArr != null && fileStatusArr.length > 0) {

                        for (FileStatus cur : fileStatusArr) {
                            files.add(cur);
                        }
                    }
                }


                //遍历所有文件
                if (files.size() > 0) {

                    logger.info(path + "找到文件" + files.size());

                    FSDataInputStream inputStream = null;
                    OutputStream outputStream = res.getOutputStream();

                    int len = files.size();
                    int i = 1;
                    long allPosition = 0;
                    for (FileStatus cur : files) {


                        logger.info("[" + i++ + "/" + len + "]" + path + ",读取文件" + cur);
                        inputStream = fs.open(cur.getPath());


                        if (position > 0) {


                            if (allPosition + cur.getLen() > position) {
                                inputStream.seek(position - allPosition);
                                logger.info("seek position " + (position - allPosition));
                                position = -1;
                            }
                            allPosition += cur.getLen();
                        }
                        org.apache.commons.io.IOUtils.copyLarge(inputStream, outputStream);
                        inputStream.close();

                    }
                    outputStream.flush();
                    outputStream.close();
                    return 200;


                } else {
                    logger.info(path + "没有找到文件" + files.size());
                }


            } else {

                return 400;
            }


        } catch (Exception e) {
            e.printStackTrace();

        }

        return 500;
    }
}
