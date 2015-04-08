import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.SocketException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class SlowMultipartTest implements Runnable {
    private static final String EOL = "\r\n";
    private static final String HYPHENS = "--";
    private static final String BOUNDARY = String.format("%x", new Random().hashCode());
    private static final String NAME = "file";
    private static final String DEFAULT_SLEEP_MS = "100";
    private static final String DEFAULT_BUFFER_BYTES = "1024";
    private static final String DEFAULT_TEST_TIME_SEC = "0";
    private static final String DEFAULT_PARALLELISM = "1";
    private static final String DEFAULT_OUTPUT_INTERVAL_SEC = "1";

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("u", "url", true, "Target URL");
        options.addOption("m", "maxsleep", true, "Max sleep (ms). default: " + DEFAULT_SLEEP_MS);
        options.addOption("n", "minsleep", true, "Min sleep (ms). default: " + DEFAULT_SLEEP_MS);
        options.addOption("b", "buffer", true, "Buffer size (Bytes). defult: " + DEFAULT_BUFFER_BYTES);
        options.addOption("l", "list", true, "File list (line by line text file)");
        options.addOption("t", "time", true, "Test time (sec). default: " + DEFAULT_TEST_TIME_SEC);
        options.addOption("p", "parallelism", true, "Parallelism. default: " + DEFAULT_PARALLELISM);
        options.addOption("i", "interval", true, "Output interval (sec). default: " + DEFAULT_OUTPUT_INTERVAL_SEC);

        try {
            BasicParser parser = new BasicParser();
            CommandLine commandLine = parser.parse(options, args);

            SlowMultipartTest test = new SlowMultipartTest();
            test.targetUrl = commandLine.getOptionValue("u");
            test.maxSleepMs = Long.parseLong(commandLine.getOptionValue("m", DEFAULT_SLEEP_MS));
            test.minSleepMs = Long.parseLong(commandLine.getOptionValue("n", DEFAULT_SLEEP_MS));
            test.bufferBytes = Integer.parseInt(commandLine.getOptionValue("b", DEFAULT_BUFFER_BYTES));
            test.fileNameList = parseFileNameList(commandLine.getOptionValue("l"));
            test.testTimeMs = TimeUnit.SECONDS.toMillis(Long.parseLong(commandLine.getOptionValue("t", DEFAULT_TEST_TIME_SEC)));
            test.parallelism = Math.max(1, Integer.parseInt(commandLine.getOptionValue("p", DEFAULT_PARALLELISM)));
            test.outputIntervalMs = TimeUnit.SECONDS.toMillis(Long.parseLong(commandLine.getOptionValue("i", DEFAULT_OUTPUT_INTERVAL_SEC)));

            if (test.targetUrl == null || test.targetUrl.isEmpty()) {
                System.err.println("ERROR: Target URL (-u,--url) is empty.");
                return;
            }
            if (test.fileNameList == null || test.fileNameList.isEmpty()) {
                System.err.println("ERROR: File list (-l,--list) is empty.");
                return;
            }

            test.test();
        } catch (ParseException e) {
            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp("SlowMultipartTest", options);
        }
    }

    @Override
    public void run() {
        if (fileNameList == null || fileNameList.isEmpty()) {
            return;
        }
        int size = fileNameList.size();
        while (System.currentTimeMillis() - startTime < testTimeMs) {
            if (postFile(fileNameList.get((int)(size * Math.random())))) {
                synchronized(okCount) {
                    okCount++;
                }
            }
        }
    }

    private void test() {
        try {
            List<Thread> threadList = new ArrayList<Thread>();
            for (int i = 0; i < parallelism; i++) {
                threadList.add(new Thread(this));
            }
            startTime = System.currentTimeMillis();
            for (int i = 0; i < parallelism; i++) {
                threadList.get(i).start();
            }
            if (outputIntervalMs > 0) {
                Thread.sleep(outputIntervalMs);
                while (System.currentTimeMillis() - startTime < testTimeMs) {
                    outputTimeRecord(false);
                    Thread.sleep(outputIntervalMs);
                }
            }
            outputTimeRecord(true);
            System.out.print("Terminating...");
            for (int i = 0; i < parallelism; i++) {
                threadList.get(i).join();
            }
            System.out.println("Finished!");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private boolean postFile(String fileName) {
        if (targetUrl == null || targetUrl.isEmpty() || fileName == null || fileName.isEmpty()) {
            return false;
        }

        boolean ok = false;
        BufferedReader reader = null;
        HttpURLConnection connection = null;
        DataOutputStream outputStream = null;
        try {
            reader = new BufferedReader(new FileReader(new File(fileName)));

            long sleep = minSleepMs + (long)((maxSleepMs - minSleepMs) * Math.random());
            long start = System.nanoTime();

            URL url = new URL(targetUrl);
            connection = (HttpURLConnection)url.openConnection();
            connection.setUseCaches(false);
            connection.setDoOutput(true);
            connection.setChunkedStreamingMode(bufferBytes);
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + BOUNDARY);

            outputStream = new DataOutputStream(connection.getOutputStream());
            outputStream.writeBytes(HYPHENS + BOUNDARY + EOL);
            outputStream.writeBytes("Content-Disposition: form-data; name=\"" + NAME + "\"; filename=\"" + fileName + "\"" + EOL);
            outputStream.writeBytes(EOL);
            outputStream.flush();

            long totalSleep = 0;
            char[] buffer = new char[bufferBytes];
            int len;
            while ((len = reader.read(buffer, 0, bufferBytes)) >= 0) {
                if (System.currentTimeMillis() - startTime >= testTimeMs) {
                    reader.close();
                    throw new TimeoutException();
                }
                outputStream.write(String.valueOf(buffer).getBytes(), 0, len);
                outputStream.flush();
                Thread.sleep(sleep);
                totalSleep += sleep;
            }

            outputStream.writeBytes(EOL);
            outputStream.writeBytes(HYPHENS + BOUNDARY + HYPHENS + EOL);
            outputStream.flush();

            int responseCode = connection.getResponseCode();
            long finish = System.nanoTime();

            if (responseCode == HttpURLConnection.HTTP_OK) {
                ok = true;
            }
            synchronized(timeRecorder) {
                timeRecorder.record(finish - start, TimeUnit.MILLISECONDS.toNanos(totalSleep));
            }
        } catch (SocketException | TimeoutException e) {
            // ignore
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            try {
                if (outputStream != null) {
                    outputStream.close();
                }
                if (connection != null) {
                    connection.disconnect();
                }
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return ok;
    }

    private int prevCount = 0;
    private double prevTime = 0.0;
    private void outputTimeRecord(boolean finish) {
        int count = 0;
        double time = (System.currentTimeMillis() - startTime) / 1000.0;
        synchronized(timeRecorder) {
            count = timeRecorder.getCount();
            if (finish) {
                double qps = count / time;
                System.out.printf("Parallelism: %d, Count: %d (OK: %d), Time: %fs, Total QPS: %f\n",
                        parallelism, count, okCount, time, qps);
            } else {
                double qps = (count - prevCount) / (time - prevTime);
                System.out.printf("Parallelism: %d, Count: %d (OK: %d), Time: %ds, Current QPS: %f\n",
                        parallelism, count, okCount, (int)time, qps);
            }
            System.out.printf("Avg: %dms (non-sleep: %dms), Max: %dms (non-sleep: %dms), Min: %dms (non-sleep: %dms)\n\n",
                    TimeUnit.NANOSECONDS.toMillis(timeRecorder.getAvg()),
                    TimeUnit.NANOSECONDS.toMillis(timeRecorder.getNonSleepAvg()),
                    TimeUnit.NANOSECONDS.toMillis(timeRecorder.getMax()),
                    TimeUnit.NANOSECONDS.toMillis(timeRecorder.getNonSleepMax()),
                    TimeUnit.NANOSECONDS.toMillis(timeRecorder.getMin()),
                    TimeUnit.NANOSECONDS.toMillis(timeRecorder.getNonSleepMin()));
        }
        System.out.flush();
        prevCount = count;
        prevTime = time;
    }

    private static List<String> parseFileNameList(String listFileName) {
        if (listFileName == null) {
            return null;
        }

        List<String> list = new ArrayList<String>();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(new File(listFileName)));
            String fileName;
            while ((fileName = reader.readLine()) != null) {
                if (!fileName.isEmpty()) {
                    list.add(fileName);
                }
            }
        } catch (FileNotFoundException e) {
            System.err.println("File(" + listFileName + ") does not exist.");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    private String targetUrl = null;
    private long maxSleepMs = 0;
    private long minSleepMs = 0;
    private int bufferBytes = 0;
    private List<String> fileNameList = null;
    private long testTimeMs = 0;
    private int parallelism = 0;
    private long outputIntervalMs = 0;
    private long startTime = 0;
    private TimeRecorder timeRecorder = new TimeRecorder();
    private Integer okCount = 0;
}
