package org.babich.sort;

import org.apache.commons.cli.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static java.lang.System.exit;
import static java.lang.System.out;

/**
 * The program should be able to run with a memory constraint of 100MB i.e. the -Xmx100m.
 * <pre>
 *  -incomingData ~/tmp/random-int.txt (1G test file = 10_000_000 elements)
 *  -result ~/tmp/sorted-int.txt
 *  -batchSize 10_000_000  (better size of batch for -Xmx100m)
 *  </pre>
 * This application use hard drive storage to external store data and implemented sort-merge strategy.
 * Sorted data batches are saved in temporary files as binary data format and looks like:
 * <pre>
 *  /var/folders/1r/_8mbjq0d265g28bpcjlf02yh0000gn/T/sorter3466698337414917773
 * </pre>
 * The complexity of the sorting algorithm:
 * <pre>
 *  Q(n log(k))
 *      k - batchSize
 *  </pre>
 * what is looks like linear
 *
 * @author Vadim Babich
 */
public class Application {

    private static final String USAGE_STRING = "java -jar external-sorting-{version}.jar [-help]" +
            " [-incomingData] [-result] [-batchSize] [generate]";

    private static final Options options;

    static {
        options = new Options();

        options.addOption(Option.builder("help")
                .required(false)
                .hasArg(false)
                .desc("print this message")
                .build());

        options.addOption(Option.builder("incomingData")
                .required(true)
                .hasArg(true)
                .desc("Path to location of the file with incoming data.")
                .build()
        );

        options.addOption(Option.builder("result")
                .required(false)
                .hasArg(true)
                .desc("Path to location of the file with outgoing data by default application directory.")
                .build()
        );

        options.addOption(Option.builder("batchSize")
                .required(false)
                .hasArg(true)
                .desc("batchSize is the number of items that can be processed in memory default 1_000_000.")
                .build()
        );

        options.addOption(Option.builder("generate")
                .required(false)
                .hasArg(true)
                .desc("use to generate random data with a given number of elements. The result will be placed in the output file.")
                .build()
        );

    }

    private static Path inputFile;
    private static Path outputFile;
    private static int batchSize;
    private static long generateAmount;

    public static void main(String[] args) {
        try {
            setupShutdownHook(out);

            init(args);

            run();
        } catch (Exception e) {
            printUsage(e.getMessage());
        }
    }

    private static void run() throws Exception {

        if(generateAmount > 0){
            generateRandomData();
            return;
        }

        out.println("started sorting data from {" + inputFile + "}");
        out.println("the result will be placed in {" + outputFile + "}");
        out.println("batch size of processed elements in memory: " + batchSize);

        try (Stream<String> stream = Files.lines(inputFile);
             BufferedWriter writer = Files.newBufferedWriter(outputFile)) {

            Collector<Integer, Sorter, Merger> sortedParticleCollector = Sorter.getSortedParticleCollector(batchSize);

            long[] incomingDataAmount = new long[]{0};
            long[] sortedDataAmount = new long[]{0};
            stream.map(Integer::parseInt)
                    .peek(item -> incomingDataAmount[0]++)
                    .collect(sortedParticleCollector)
                    .doMergeIn(writeResultToOutput(writer).andThen(v -> sortedDataAmount[0]++));

            out.println("Total items found in the incoming data file:" + incomingDataAmount[0]);
            out.println("Total items were put in the result:" + sortedDataAmount[0]);
        }

        out.println("sorting data completed.");
    }

    static Consumer<Integer> writeResultToOutput(BufferedWriter writer){
        return value -> {
            try {
                writer.write(String.valueOf(value));
                writer.newLine();
            } catch (IOException e) {
                throw new UncheckedIOException("Cannot write data to output file", e);
            }
        };
    }

    static void generateRandomData() {
        out.println("start generating data");
        out.println("the result will be placed in {" + outputFile + "}");
        out.println("amount of output elements: " + generateAmount);

        Random random = new Random();
        try (BufferedWriter writer = Files.newBufferedWriter(outputFile)) {
            for (int i = 0; i < generateAmount; i++) {
                writer.write(String.valueOf(random.nextInt()));
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        out.println("generating data completed.");
    }

    private static void init(String... args) {
        out.println("initializing..");

        CommandLine commandLine;
        try {
            commandLine = new DefaultParser().parse(options, args);
        } catch (ParseException e) {
            throw new IllegalArgumentException(e.getMessage());
        }

        if (commandLine.hasOption("help")) {
            printUsage();
            exit(0);
        }

        setUpInputFile(commandLine.getOptionValue("incomingData"));

        setUpGenerateAmount(commandLine.getOptionValue("generate"));

        setUpOutputFile(commandLine.getOptionValue("result"));

        setUpBatchSize(commandLine.getOptionValue("batchSize"));
    }

    private static void setUpInputFile(String path) {
        inputFile = Paths.get(path).normalize().toAbsolutePath();
        if (inputFile.toFile().isDirectory()) {
            throw new IllegalArgumentException("Invalid incoming data path, file cannot be a directory.");
        }
        if (!inputFile.toFile().exists()) {
            throw new IllegalArgumentException(String
                    .format("Invalid incoming data path {%s}, file does not exist.", inputFile));
        }
    }

    private static void setUpOutputFile(String path) {
        if (null == path) {
            path = Paths.get(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")) + ".txt")
                    .toString();
        }
        outputFile = Paths.get(path).normalize().toAbsolutePath();
        if (outputFile.toFile().isDirectory()) {
            throw new IllegalArgumentException(String
                    .format("Invalid outgoing data path {%s}, file cannot be a directory.", outputFile));
        }
    }

    private static void setUpBatchSize(String value) {
        if (null == value) {
            batchSize = 5_000_000;
            return;
        }
        batchSize = Integer.parseInt(value);
    }

    private static void setUpGenerateAmount(String value){
        if(null == value){
            generateAmount = 0;
            return;
        }
        generateAmount = Long.parseLong(value);
    }

    private static void printUsage(String message) {
        out.println();
        out.println(message);
        printUsage();
    }

    private static void printUsage() {
        out.println();

        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp(120, USAGE_STRING, "", options, "");
        out.println();
    }

    private static void setupShutdownHook(PrintStream printStream) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                Thread.sleep(400);
            } catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
            }
            printStream.println("\nShutting down ...");
        }));
    }
}
