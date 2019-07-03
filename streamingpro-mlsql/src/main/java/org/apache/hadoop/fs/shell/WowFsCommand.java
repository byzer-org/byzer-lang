/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.shell;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import streaming.common.PathFun;

import java.io.IOException;
import java.io.PrintStream;
import java.util.LinkedList;

/**
 * Base class for all "hadoop fs" commands
 */

@InterfaceAudience.Private
@InterfaceStability.Evolving

// this class may not look useful now, but it's a placeholder for future
// functionality to act as a registry for fs commands.  currently it's being
// used to implement unnecessary abstract methods in the base class

abstract public class WowFsCommand extends Command {
    /**
     * Register the command classes used by the fs subcommand
     *
     * @param factory where to register the class
     */
    public static void registerCommands(CommandFactory factory) {
        factory.registerCommands(WowLs.class);
    }


    // historical abstract method in Command
    @Override
    public String getCommandName() {
        return getName();
    }

    // abstract method that normally is invoked by runall() which is
    // overridden below
    @Override
    protected void run(Path path) throws IOException {
        throw new RuntimeException("not supposed to get here");
    }

    protected String basePath;

    public WowFsCommand(Configuration conf, String basePath, PrintStream out, PrintStream error) {
        super(conf);
        this.out = out;
        this.err = error;
        this.basePath = basePath;
    }

    public void redefineBaseDir(LinkedList<String> args) {
        LinkedList<String> temp = new LinkedList<>();
        for (String arg : args) {
            String[] paths = arg.split("/");
            for (String path : paths) {
                if (path.trim().equals("..")) {
                    throw new RuntimeException("path should not contains ..");
                }
            }
            temp.add(new PathFun(this.basePath).add(arg).toPath());
        }
        args.clear();
        args.addAll(temp);
    }

    public String cleanPath(String path) {
        return path.substring(this.basePath.length() - 1);
    }

    /**
     * @deprecated use {@link Command#run(String...argv)}
     */
    @Deprecated
    @Override
    public int runAll() {
        return run(args);
    }
}
