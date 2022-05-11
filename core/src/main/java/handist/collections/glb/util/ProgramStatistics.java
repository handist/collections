package handist.collections.glb.util;

import static handist.collections.glb.GlobalLoadBalancer.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Consumer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import handist.collections.dist.DistLog.LogItem;
import handist.collections.glb.GlobalLoadBalancer;
import handist.collections.util.SavedLog;

public class ProgramStatistics {

    private static final String OPT_WORKER_OVER_TIME = "w";

    private static Options commandOptions() {
        final Options opts = new Options();
        opts.addOption(OPT_WORKER_OVER_TIME, "worker", true,
                "produces a CSV showing the proportion of workers active over time");
        opts.addOption("f", false, "if the generation of files would result in some being overwritten, this program "
                + "normally skips these files; use this option to force the overwriting of existing files");
        opts.addOption("v", false,
                "verbose output; if activated, will print the contents of the log on the standard output");
        return opts;
    }

    /**
     * Program which obtains a GlbLog from a file and produces various CSV exports
     * on demand.
     *
     * @param args (option)+ <log file>
     */
    public static void main(String[] args) {
        // Check that at least the input log file is specified
        if (args.length <= 1) {
            final Options opt = commandOptions();
            new HelpFormatter().printHelp("(options)+ <log input file>", opt);
            return;
        }

        // Open the input log file
        SavedLog log = null;
        final String inputFileName = args[args.length - 1];
        final File f = new File(inputFileName);
        if (!f.exists()) {
            System.err.println("Could not read file " + inputFileName);
            System.err.println("Exiting with code -1");
            System.exit(-1);
        }
        try {
            log = new SavedLog(f);
        } catch (final Exception e) {
            System.err.println("A problem occurred while parsing input file " + inputFileName);
            e.printStackTrace();
            System.exit(-2);
        }
        final ProgramStatistics statFactory = new ProgramStatistics(log);

        // Now that the input file has been successfully parsed,
        // check which program outputs are desired
        final Options programOptions = commandOptions();
        final CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        final String[] optionArray = Arrays.copyOf(args, args.length - 1); // remove the input file from the options
        try {
            cmd = parser.parse(programOptions, optionArray);
        } catch (final ParseException e1) {
            System.err.println(e1.getLocalizedMessage());
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(
                    "java [...] " + ProgramStatistics.class.getCanonicalName() + " [options]* <input log file>",
                    programOptions);
            return;
        }

        if (cmd.hasOption("v")) {
            log.printAll(System.out);
        }

        // Check if the "force overwrite" option is set
        final boolean overwriteFiles = cmd.hasOption("f");

        // Produce each output in the specified files
        makeOutputToFile(cmd, OPT_WORKER_OVER_TIME, statFactory::workerActivity, overwriteFiles);
    }

    /**
     * Generic method used to check whether an option was set
     *
     * @param option
     * @param outputMethod
     */
    private static void makeOutputToFile(CommandLine cmd, String option, Consumer<PrintStream> outputMethod,
            boolean overwriteFiles) {
        if (cmd.hasOption(option)) {
            final File output = new File(cmd.getOptionValue(option));
            if (output.exists() && !overwriteFiles) {
                System.err.println("File " + output.getAbsolutePath() + " already exists, skipping");
            } else {
                PrintStream ps;
                try {
                    ps = new PrintStream(output);
                    outputMethod.accept(ps);
                    ps.close();
                } catch (final FileNotFoundException e) {
                    System.err.println("A problem occurred while writing to " + output.getAbsolutePath());
                    e.printStackTrace();
                }
                System.out.println("Wrote file " + output.getAbsolutePath());
            }
        }
    }

    /**
     * Logger of a GLB execution. Is used by this class to produce various data
     * tables to generates various plots about the glb execution considered
     */
    final private SavedLog log;

    /**
     * Constructor
     *
     * @param logger DistLog instance retrieved through method
     *               {@link GlobalLoadBalancer#getPreviousLog()}
     */
    public ProgramStatistics(SavedLog logger) {
        log = logger;
    }

    /**
     * Produces a data output of the number of workers running, yielding, and
     * inactive on each host over time.
     *
     * @param ps the printstream onto which the data needs to be printed
     */
    public void workerActivity(PrintStream ps) {
        ps.println("# TimeStamp(s) InactiveWorker YieldingWorker RunningWorker");
        // First obtain the entire program computation time
        long programStart = 0, programEnd = -1;
        final Collection<LogItem> underGlbItems = log.getLog(0, LOGKEY_UNDER_GLB, 0);
        for (final LogItem item : underGlbItems) {
            switch (item.msg) {
            case LOG_PROGRAM_STARTED:
                programStart = Long.parseLong(item.appendix);
                break;
            case LOG_PROGRAM_ENDED:
                programEnd = Long.parseLong(item.appendix);
                break;
            default:
                // Other potential items ignored
            }
        }
        final long TOTAL_COMPUTATION_TIME = programEnd - programStart;

        // For each host in the computation
        for (int place = 0; place < log.placeCount(); place++) {
            ps.println("# place(" + place + ")");

            int workerCount = -1;
            long referenceNanoTime = -1l;
            for (final LogItem item : log.getLog(place, LOGKEY_GLB, 0)) {
                switch (item.msg) {
                case LOG_INITIALIZED_AT_NANOTIME:
                    referenceNanoTime = Long.parseLong(item.appendix);
                    break;
                case LOG_INITIALIZED_WORKERS:
                    workerCount = Integer.parseInt(item.appendix);
                    break;
                default:
                    // Other messages are ignored
                }

            }

            // At first, all workers are inactive
            int inactive = workerCount;
            int yielding = 0;
            int running = 0;

            // First line output
            ps.println(String.format("%s %s %s %s", 0l, inactive, yielding, running));

            // For each "worker" event, update and print the new worker status
            final Collection<LogItem> workerEvents = log.getLog(place, LOGKEY_WORKER, 0);
            if (workerEvents != null) {
                for (final LogItem item : log.getLog(place, LOGKEY_WORKER, 0)) {
                    // FIXME add the update of the timestamp
                    final double stamp = (Long.parseLong(item.appendix) - referenceNanoTime) / 1e9;
                    switch (item.msg) {
                    case LOG_WORKER_STARTED:
                        inactive--;
                        running++;
                        break;
                    case LOG_WORKER_YIELDING:
                        yielding++;
                        running--;
                        break;
                    case LOG_WORKER_RESUMED:
                        yielding--;
                        running++;
                    case LOG_WORKER_STOPPED:
                        running--;
                        inactive++;
                    default:
                        // in other cases, ignore the logged entry
                    }
                    // Even if there were no changes, this has no adverse effect on the plot
                    ps.println(String.format("%s %s %s %s", stamp, inactive, yielding, running));
                }
            }

            // Last line with "end of computation"
            ps.println(String.format("%s %s %s %s", TOTAL_COMPUTATION_TIME / 1e9, inactive, yielding, running));

            // Add two empty line to separate the data from each host
            ps.println();
            ps.println();
        }
    }
}
