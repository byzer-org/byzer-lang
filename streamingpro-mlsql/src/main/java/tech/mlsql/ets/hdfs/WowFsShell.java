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

package tech.mlsql.ets.hdfs;

import org.apache.commons.lang.WordUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.shell.*;
import org.apache.hadoop.tools.TableListing;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

/**
 * 2019-05-07 WilliamZhu(allwefantasy@gmail.com)
 */
public class WowFsShell extends Configured implements Tool {

    static final Log LOG = LogFactory.getLog(WowFsShell.class);

    private static final int MAX_LINE_WIDTH = 80;

    private FileSystem fs;
    protected String basePath;

    protected CommandFactory commandFactory;

    private final ByteArrayOutputStream outS = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errorS = new ByteArrayOutputStream();
    public final PrintStream out = new PrintStream(outS);
    public final PrintStream error = new PrintStream(errorS);

    public String getOut() {
        return _out(outS);
    }

    public String _out(ByteArrayOutputStream wow) {
        try {
            String temp = wow.toString("UTF8");
            wow.reset();
            return temp;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    public String getError() {
        return _out(errorS);
    }

    private final String usagePrefix =
            "Usage: hadoop fs [generic options]";

    /**
     * Default ctor with no configuration.  Be sure to invoke
     * {@link #setConf(Configuration)} with a valid configuration prior
     * to running commands.
     */
    public WowFsShell() {
        this(null, null);
    }

    /**
     * Construct a FsShell with the given configuration.  Commands can be
     * executed via {@link #run(String[])}
     *
     * @param conf the hadoop configuration
     */
    public WowFsShell(Configuration conf, String basePath) {
        super(conf);
        if(!StringUtils.isBlank(basePath)) {
            this.basePath = basePath;
        } else {
            this.basePath = "/";
        }

    }

    protected FileSystem getFS() throws IOException {
        if (fs == null) {
            fs = FileSystem.get(getConf());
        }
        return fs;
    }

    protected void init() throws IOException {
        getConf().setQuietMode(true);
        if (commandFactory == null) {
            commandFactory = new WowCommandFactory(getConf());
            commandFactory.addObject(new WowFsShell.Help(getConf(), basePath, out, error), "-help");
            commandFactory.addObject(new WowFsShell.Usage(getConf(), basePath, out, error), "-usage");
            commandFactory.addObject(new WowLs(getConf(), basePath, out, error), "-ls");
            commandFactory.addObject(new WowLs.Lsr(getConf(), basePath, out, error), "-lsr");
            commandFactory.addObject(new WowDelete.Rm(getConf(), basePath, out, error), "-rm");
            commandFactory.addObject(new WowDelete.Rmdir(getConf(), basePath, out, error), "-rmdir");
            commandFactory.addObject(new WowDelete.Rmr(getConf(), basePath, out, error), "-rmr");
            commandFactory.addObject(new WowMoveCommands.Rename(getConf(), basePath, out, error), "-mv");
            commandFactory.addObject(new WowMkdir(getConf(), basePath, out, error), "-mkdir");
            commandFactory.addObject(new WowCopyCommands.Merge(getConf(), basePath, out, error), "-getmerge");
            commandFactory.addObject(new WowCopyCommands.Cp(getConf(), basePath, out, error), "-cp");
            commandFactory.addObject(new WowCount(getConf(), basePath, out, error), "-count");

        }
    }


    // NOTE: Usage/Help are inner classes to allow access to outer methods
    // that access commandFactory

    /**
     * Display help for commands with their short usage and long description
     */
    protected class Usage extends WowFsCommand {
        public static final String NAME = "usage";
        public static final String USAGE = "[cmd ...]";
        public static final String DESCRIPTION =
                "Displays the usage for given command or all commands if none " +
                        "is specified.";


        public Usage(Configuration conf, String basePath, PrintStream out, PrintStream error) {
            super(conf, basePath, out, error);
        }

        @Override
        protected void processRawArguments(LinkedList<String> args) {
            if (args.isEmpty()) {
                printUsage(out);
            } else {
                for (String arg : args) printUsage(out, arg);
            }
        }
    }

    /**
     * Displays short usage of commands sans the long description
     */
    protected class Help extends WowFsCommand {
        public static final String NAME = "help";
        public static final String USAGE = "[cmd ...]";
        public static final String DESCRIPTION =
                "Displays help for given command or all commands if none " +
                        "is specified.";

        public Help(Configuration conf, String basePath, PrintStream out, PrintStream error) {
            super(conf, basePath, out, error);
        }

        @Override
        protected void processRawArguments(LinkedList<String> args) {
            if (args.isEmpty()) {
                printHelp(out);
            } else {
                for (String arg : args) printHelp(out, arg);
            }
        }
    }

    /*
     * The following are helper methods for getInfo().  They are defined
     * outside of the scope of the Help/Usage class because the run() method
     * needs to invoke them too.
     */

    // print all usages
    private void printUsage(PrintStream out) {
        printInfo(out, null, false);
    }

    // print one usage
    private void printUsage(PrintStream out, String cmd) {
        printInfo(out, cmd, false);
    }

    // print all helps
    private void printHelp(PrintStream out) {
        printInfo(out, null, true);
    }

    // print one help
    private void printHelp(PrintStream out, String cmd) {
        printInfo(out, cmd, true);
    }

    private void printInfo(PrintStream out, String cmd, boolean showHelp) {
        if (cmd != null) {
            // display help or usage for one command
            Command instance = commandFactory.getInstance("-" + cmd);
            if (instance == null) {
                throw new WowFsShell.UnknownCommandException(cmd);
            }
            if (showHelp) {
                printInstanceHelp(out, instance);
            } else {
                printInstanceUsage(out, instance);
            }
        } else {
            // display help or usage for all commands
            out.println(usagePrefix);

            // display list of short usages
            ArrayList<Command> instances = new ArrayList<Command>();
            for (String name : commandFactory.getNames()) {
                Command instance = commandFactory.getInstance(name);
                if (!instance.isDeprecated()) {
                    out.println("\t[" + instance.getUsage() + "]");
                    instances.add(instance);
                }
            }
            // display long descriptions for each command
            if (showHelp) {
                for (Command instance : instances) {
                    out.println();
                    printInstanceHelp(out, instance);
                }
            }
            out.println();
            ToolRunner.printGenericCommandUsage(out);
        }
    }

    private void printInstanceUsage(PrintStream out, Command instance) {
        out.println(usagePrefix + " " + instance.getUsage());
    }

    private void printInstanceHelp(PrintStream out, Command instance) {
        out.println(instance.getUsage() + " :");
        TableListing listing = null;
        final String prefix = "  ";
        for (String line : instance.getDescription().split("\n")) {
            if (line.matches("^[ \t]*[-<].*$")) {
                String[] segments = line.split(":");
                if (segments.length == 2) {
                    if (listing == null) {
                        listing = createOptionTableListing();
                    }
                    listing.addRow(segments[0].trim(), segments[1].trim());
                    continue;
                }
            }

            // Normal literal description.
            if (listing != null) {
                for (String listingLine : listing.toString().split("\n")) {
                    out.println(prefix + listingLine);
                }
                listing = null;
            }

            for (String descLine : WordUtils.wrap(
                    line, MAX_LINE_WIDTH, "\n", true).split("\n")) {
                out.println(prefix + descLine);
            }
        }

        if (listing != null) {
            for (String listingLine : listing.toString().split("\n")) {
                out.println(prefix + listingLine);
            }
        }
    }

    // Creates a two-row table, the first row is for the command line option,
    // the second row is for the option description.
    private TableListing createOptionTableListing() {
        return new TableListing.Builder().addField("").addField("", true)
                .wrapWidth(MAX_LINE_WIDTH).build();
    }

    /**
     * run
     */
    @Override
    public int run(String argv[]) throws Exception {
        // initialize FsShell
        init();

        int exitCode = -1;
        if (argv.length < 1) {
            printUsage(error);
        } else {
            String cmd = argv[0];
            Command instance = null;
            try {
                instance = commandFactory.getInstance(cmd);
                if (instance == null) {
                    throw new WowFsShell.UnknownCommandException();
                }
                exitCode = instance.run(Arrays.copyOfRange(argv, 1, argv.length));
            } catch (IllegalArgumentException e) {
                displayError(cmd, e.getLocalizedMessage());
                if (instance != null) {
                    printInstanceUsage(error, instance);
                }
            } catch (Exception e) {
                // instance.run catches IOE, so something is REALLY wrong if here
                LOG.debug("Error", e);
                displayError(cmd, "Fatal internal error");
                e.printStackTrace(error);
            }
        }
        return exitCode;
    }

    private void displayError(String cmd, String message) {
        for (String line : message.split("\n")) {
            error.println(cmd + ": " + line);
            if (cmd.charAt(0) != '-') {
                Command instance = null;
                instance = commandFactory.getInstance("-" + cmd);
                if (instance != null) {
                    error.println("Did you mean -" + cmd + "?  This command " +
                            "begins with a dash.");
                }
            }
        }
    }

    /**
     * Performs any necessary cleanup
     *
     * @throws IOException upon error
     */
    public void close() throws IOException {
        outS.close();
        out.close();
        errorS.close();
        error.close();
    }

    /**
     * The default ctor signals that the command being executed does not exist,
     * while other ctor signals that a specific command does not exist.  The
     * latter is used by commands that process other commands, ex. -usage/-help
     */
    @SuppressWarnings("serial")
    static class UnknownCommandException extends IllegalArgumentException {
        private final String cmd;

        UnknownCommandException() {
            this(null);
        }

        UnknownCommandException(String cmd) {
            this.cmd = cmd;
        }

        @Override
        public String getMessage() {
            return ((cmd != null) ? "`" + cmd + "': " : "") + "Unknown command";
        }
    }
}
