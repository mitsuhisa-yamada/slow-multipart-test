import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
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
    private static final String DEFAULT_CONNECT_TIMEOUT_MS = "1000";
    private static final String DEFAULT_READ_TIMEOUT_MS = "1000";

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
        options.addOption("c", "connecttimeout", true, "Connect timeout (ms). default: " + DEFAULT_CONNECT_TIMEOUT_MS);
        options.addOption("r", "readtimeout", true, "Read timeout (ms). default: " + DEFAULT_READ_TIMEOUT_MS);

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
            test.connectTimeoutMs = Integer.parseInt(commandLine.getOptionValue("c", DEFAULT_CONNECT_TIMEOUT_MS));
            test.readTimeoutMs = Integer.parseInt(commandLine.getOptionValue("r", DEFAULT_READ_TIMEOUT_MS));

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
            postFile(fileNameList.get((int)(size * Math.random())));
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
                long timespent = System.currentTimeMillis() - startTime;
                long timeleft = testTimeMs - timespent;
                while (timeleft > 0) {
                    Thread.sleep(Math.min(outputIntervalMs - timespent % outputIntervalMs, timeleft));
                    outputTimeRecord(false);
                    timespent = System.currentTimeMillis() - startTime;
                    timeleft = testTimeMs - timespent;
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

    private void postFile(String fileName) {
        if (targetUrl == null || targetUrl.isEmpty() || fileName == null || fileName.isEmpty()) {
            return;
        }

        BufferedReader reader = null;
        HttpURLConnection connection = null;
        DataOutputStream outputStream = null;
        boolean connecting = false, writing = false, reading = false;
        try {
            reader = new BufferedReader(new FileReader(new File(fileName)));

            long sleep = minSleepMs + (long)((maxSleepMs - minSleepMs) * Math.random());
            long start = System.nanoTime();

            URL url = new URL(targetUrl);
            connection = (HttpURLConnection)url.openConnection();
            connection.setUseCaches(false);
            connection.setDoOutput(true);
            connection.setChunkedStreamingMode(bufferBytes);
            connection.setConnectTimeout(connectTimeoutMs);
            connection.setReadTimeout(readTimeoutMs);
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + BOUNDARY);

            connecting = true;
            outputStream = new DataOutputStream(connection.getOutputStream());
            connecting = false;
            writing = true;
            outputStream.writeBytes(HYPHENS + BOUNDARY + EOL);
            outputStream.writeBytes("Content-Disposition: form-data; name=\"" + NAME + "\"; filename=\"" + fileName + "\"" + EOL);
            outputStream.writeBytes(EOL);
            outputStream.flush();
            writing = false;

            long totalSleep = 0;
            char[] buffer = new char[bufferBytes];
            int len;
            while ((len = reader.read(buffer, 0, bufferBytes)) >= 0) {
                if (System.currentTimeMillis() - startTime >= testTimeMs) {
                    throw new TimeoutException();
                }
                writing = true;
                outputStream.write(String.valueOf(buffer).getBytes(), 0, len);
                outputStream.flush();
                writing = false;
                Thread.sleep(sleep);
                totalSleep += sleep;
            }

            writing = true;
            outputStream.writeBytes(EOL);
            outputStream.writeBytes(HYPHENS + BOUNDARY + HYPHENS + EOL);
            outputStream.flush();
            writing = false;

            reading = true;
            int responseCode = connection.getResponseCode();
            reading = false;
            long finish = System.nanoTime();

            synchronized(timeRecorder) {
                timeRecorder.record(finish - start, TimeUnit.MILLISECONDS.toNanos(totalSleep));
                if (responseCode == HttpURLConnection.HTTP_OK) {
                    okCount++;
                }
            }
        } catch (FileNotFoundException e) {
            System.err.println("File(" + fileName + ") does not exist.");
        } catch (MalformedURLException e) {
            System.err.println("URL(" + targetUrl + ") is invalid.");
        } catch (SocketTimeoutException e) {
            if (connecting) {
                synchronized(connectTimeoutCount) {
                    connectTimeoutCount++;
                }
            } else if (reading) {
                synchronized(readTimeoutCount) {
                    readTimeoutCount++;
                }
            } else {
                e.printStackTrace();
            }
        } catch (IOException e) {
            if (connecting) {
                synchronized(connectErrorCount) {
                    connectErrorCount++;
                }
            } else if (writing) {
                synchronized(writeErrorCount) {
                    writeErrorCount++;
                }
            } else if (reading) {
                synchronized(readErrorCount) {
                    readErrorCount++;
                }
            } else {
                e.printStackTrace();
            }
        } catch (TimeoutException e) {
            // ignore
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                connection.disconnect();
            }
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
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
                System.out.printf("Parallelism: %d, Time: %ds, Total QPS: %f\n",
                        parallelism, (int)time, qps);
            } else {
                double qps = (count - prevCount) / (time - prevTime);
                System.out.printf("Parallelism: %d, Time: %ds, Current QPS: %f\n",
                        parallelism, (int)time, qps);
            }
            System.out.printf("Response: %d (OK: %d), Connect Error: %d, Write Error: %d, Read Error: %d, Connect Timeout: %d, Read Timeout: %d\n",
                    count, okCount, connectErrorCount, writeErrorCount, readErrorCount, connectTimeoutCount, readTimeoutCount);
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
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
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
    private int connectTimeoutMs = 0;
    private int readTimeoutMs = 0;
    private long startTime = 0;
    private TimeRecorder timeRecorder = new TimeRecorder();
    private int okCount = 0;
    private Integer connectErrorCount = 0;
    private Integer writeErrorCount = 0;
    private Integer readErrorCount = 0;
    private Integer connectTimeoutCount = 0;
    private Integer readTimeoutCount = 0;
}
