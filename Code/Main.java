package assign1;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleEntry;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class Main {
    public static final String WORD_FILE = "words.txt";
    public static final String IDF_FILE = "idf.csv";
    public static final String DOC_DIR = "docs/";
    public static final String TF_IDF_DIR = "TF-IDF-Docs/";
    public static final String TASK1_OUT = "task1.csv";

    public static final String S3_DOC_DIR = "cs755-group6-docs";
    public static final String S3_TF_IDF_DIR = "cs755-group6-docs-tf-idf";
    public static final String S3_KEY_LIST = "s3keys.txt";

    private static final AmazonS3 s3ClientForRead = AmazonS3Client.builder().withRegion("us-east-1").build();
    private static final BasicAWSCredentials awsCreds = new BasicAWSCredentials("AKIAIBQ7ULGY7PD5V6RA", "lGugIN7E+6Vw7c2XpP5b/Av+Qk2Op9ddyxa8SRhP");
    public static final AmazonS3 s3ClientForWrite = AmazonS3ClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(awsCreds)).withRegion("us-east-1")
            .build();

    public static void main(String[] args) {
        String bucket_name = "metcs755";
        String key_name = args[0];

//        Thread thread1 = new Thread(() -> {
//            try {
//                System.out.println("Starting Task 1");
//                doTask1(bucket_name, key_name);
//                System.out.println("Completed Task 1");
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        });
//        thread1.start();

//        Thread thread2 = new Thread(() -> {
//            try {
//                System.out.println("Starting Task 3");
//                doTask3(bucket_name, key_name);
//                System.out.println("Completed Task 3");
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        });
//        thread2.start();

        try {
            System.out.println("Starting Task 3");
            doTask3(bucket_name, key_name);
            System.out.println("Completed Task 3");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void doTask1(String bucket_name, String key_name) throws IOException {
        S3Object s3object = s3ClientForRead.getObject(bucket_name, key_name);
        S3ObjectInputStream s3is1 = s3object.getObjectContent();

        BufferedReader reader1 = new BufferedReader(new InputStreamReader(s3is1));

        Map<String, Integer> wordCount = reader1.lines().parallel()
                .map(line -> line.split(">")[1].replaceAll("<[^>]+>", ""))
                .flatMap(line -> Arrays.stream(line.trim().split(" ")))
                .map(word -> word.replaceAll("[^a-zA-Z]", "").toLowerCase().trim())
                .filter(word -> word.length() > 0)
                .map(word -> new SimpleEntry<>(word, 1))
                .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue, (v1, v2) -> v1 + v2));

        Map<String, Integer> sortedResult = wordCount.entrySet().parallelStream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .limit(5000)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (oldValue, newValue) -> oldValue, LinkedHashMap::new));

        AtomicInteger i = new AtomicInteger(0);
        Map<String, Integer[]> result = sortedResult.entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey().toUpperCase(), entry -> new Integer[]{i.getAndIncrement(), entry.getValue()}, (oldValue, newValue) -> oldValue, LinkedHashMap::new));

        Set<String> searchWords = Set.of("during", "and", "time", "protein", "car");

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(TASK1_OUT))) {
            result.entrySet().parallelStream()
                    .filter(word -> searchWords.contains(word.getKey().toLowerCase()))
                    .forEach((entry) -> {
                        try {
                            bw.write(entry.getKey() + ", " + entry.getValue()[0] + ", " + entry.getValue()[1] + "\n");
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        s3is1.close();
    }

    private static void doTask3(String bucket_name, String key_name) throws IOException {
        Files.deleteIfExists(Paths.get(WORD_FILE));
        Path dirPath = Paths.get(DOC_DIR);

        if (!Files.exists(dirPath)) {
            try {
                Files.createDirectories(dirPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        FileUtils.cleanDirectory(new File(DOC_DIR));

        dirPath = Paths.get(TF_IDF_DIR);

        if (!Files.exists(dirPath)) {
            try {
                Files.createDirectories(dirPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        FileUtils.cleanDirectory(new File(TF_IDF_DIR));
        FileUtils.deleteQuietly(new File(WORD_FILE));
        FileUtils.deleteQuietly(new File(IDF_FILE));
        FileUtils.deleteQuietly(new File(S3_KEY_LIST));
        System.out.println("Cleaned directories");

        S3Object s3object = s3ClientForRead.getObject(bucket_name, key_name);
        S3ObjectInputStream s3is1 = s3object.getObjectContent();

        BufferedReader reader1 = new BufferedReader(new InputStreamReader(s3is1));

        ExecutorService executorService = Executors.newFixedThreadPool(50);
        AtomicLong noOfDocs = new AtomicLong();

        reader1.lines().forEach(line -> {
            System.out.println("Processing Document " + noOfDocs.incrementAndGet());
            executorService.execute(new DocInfo(line));
        });

        executorService.shutdown();
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("All documents processed");

        ExecutorService wordExecutorService = Executors.newFixedThreadPool(50);
        CompletionService<String> service = new ExecutorCompletionService<>(wordExecutorService);
        Set<String> wordsBeingProcessed = new HashSet<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(WORD_FILE)))) {
            String word;
            while ((word = reader.readLine()) != null) {
                if (wordsBeingProcessed.contains(word)) {
                    continue;
                }

                boolean alreadyProcessed = false;
                if (Files.exists(Paths.get(IDF_FILE))) {
                    try (BufferedReader idfReader = new BufferedReader(new InputStreamReader(new FileInputStream(IDF_FILE)))) {
                        String line;
                        while ((line = idfReader.readLine()) != null) {
                            if (line.startsWith(word)) {
                                alreadyProcessed = true;
                                break;
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                if (alreadyProcessed) {
                    continue;
                }

                wordsBeingProcessed.add(word);
                service.submit(new IDFWorker(noOfDocs.get(), word));
            }
        }
        wordExecutorService.shutdown();

        while (!wordExecutorService.isTerminated()) {
            try {
                Future<String> future = service.poll(1, TimeUnit.MINUTES);
                if (future != null) {
                    String word = future.get();
                    //System.out.println("Finished processing the word - " + word);
                    wordsBeingProcessed.remove(word);
                } else {
                    System.out.println("Timeout");
                }
            } catch (ExecutionException | InterruptedException ex) {
                ex.printStackTrace();
            }
        }

        Files.deleteIfExists(Paths.get(WORD_FILE));
        System.out.println("IDF Calculation done for all words.");

        ExecutorService finalExecutor = Executors.newFixedThreadPool(50);
        CompletionService<String> finalService = new ExecutorCompletionService<>(finalExecutor);
        Set<String> filesBeingProcessed = new HashSet<>();
        Files.lines(Paths.get(S3_KEY_LIST))
                .forEach((path) -> {
                    if (!filesBeingProcessed.contains(path)) {
                        try {
                            s3ClientForWrite.getObjectMetadata(S3_TF_IDF_DIR, path);
                        } catch (AmazonServiceException ex) {
                            String errorCode = ex.getErrorCode();
                            if (errorCode.equals("404 Not Found")) {
                                filesBeingProcessed.add(path);
                                finalService.submit(new TF_IDFWorker(path));
                            }
                        }
                    }
                });

        finalExecutor.shutdown();

        while (!finalExecutor.isTerminated()) {
            try {
                Future<String> future = finalService.poll(1, TimeUnit.MINUTES);
                if (future != null) {
                    String fileName = future.get();
                    System.out.println("Finished processing the DOC file - " + fileName);
                    filesBeingProcessed.remove(fileName);
                } else {
                    System.out.println("Timeout");
                }
            } catch (ExecutionException | InterruptedException ex) {
                ex.printStackTrace();
            }
        }
        System.out.println("All docs are processed for TF-IDF");

        s3is1.close();

        FileUtils.deleteQuietly(new File(S3_KEY_LIST));
    }
}
