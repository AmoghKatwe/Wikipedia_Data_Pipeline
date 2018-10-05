package assign1;

import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.commons.io.FileUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.counting;

public class DocInfo implements Runnable {
    private String docID;
    private List<String> words;

    DocInfo(String line) {
        String[] lineSplitter = line.split(">");

        this.docID = lineSplitter[0].substring(lineSplitter[0].indexOf("<doc id=\"") + "<doc id=\"".length(), lineSplitter[0].indexOf("\" url=\""));
        this.words = Arrays.asList(lineSplitter[1].replaceAll("<[^>]+>", "").trim().split(" "));
        //System.out.println("Processing " + docID);
    }

    @Override
    public void run() {
        Map<String, Long> wordCount = words.parallelStream()
                .map(word -> word.replaceAll("[^a-zA-Z]", "").toLowerCase().trim())
                .filter(word -> word.length() > 0)
                .collect(Collectors.groupingBy(Function.identity(), counting()));

        Map<String, Long> sortedWordCount = wordCount.entrySet().parallelStream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .limit(20000)
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (v1, v2) -> {
                            throw new IllegalStateException();
                        },
                        LinkedHashMap::new
                ));

        Map<String, Map<String, Object>> wordCountAndTF = sortedWordCount.entrySet().parallelStream()
                .filter(entry -> calculateTF(entry.getValue()) > 0D)
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> Map.of("Count", entry.getValue(), "TF", calculateTF(entry.getValue())), (oldValue, newValue) -> oldValue, LinkedHashMap::new));

        //System.out.println("Word Count & TF Calculation done for " + docID);

        try {
            BufferedWriter bwWordFile = new BufferedWriter(new FileWriter(Main.WORD_FILE, true));
            BufferedWriter bwDocFile = new BufferedWriter(new FileWriter(Main.DOC_DIR + docID + ".csv"));

            wordCountAndTF.forEach((key, mapVal) -> {
                try {
                    bwWordFile.write(key + "\n");
                    bwDocFile.write(key + ", " + mapVal.get("Count") + ", " + mapVal.get("TF") + "\n");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            bwWordFile.close();
            bwDocFile.close();

            //System.out.println(docID + " saved after processing");

            //System.out.println("Uploading file to S3 - " + docID);
            File file = new File(Main.DOC_DIR + docID + ".csv");
            Main.s3ClientForWrite.putObject(new PutObjectRequest(Main.S3_DOC_DIR, docID + ".csv", file));
            Thread.sleep(500);

            FileUtils.deleteQuietly(file);

            BufferedWriter s3KeysFile = new BufferedWriter(new FileWriter(Main.S3_KEY_LIST, true));
            s3KeysFile.write(docID + ".csv\n");
            s3KeysFile.close();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private Double calculateTF(Long count) {
        double doubleVal = (double) count / (double) this.words.size();
        return (double) Math.round(doubleVal * 100d) / 100d;
    }

    static void calculateIDF(Long totalDocs, String word) {
        try {
            long wordCount = Files.lines(Paths.get(Main.WORD_FILE))
                    .filter(line -> line.equals(word))
                    .count();

            double idfValue = calculateIDF(totalDocs, wordCount);
            if (idfValue <= 0D) {
                return;
            }

            try (BufferedWriter bwWordFile = new BufferedWriter(new FileWriter(Main.IDF_FILE, true))) {
                bwWordFile.write(word + ", " + idfValue + "\n");
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static double calculateIDF(Long totalDocs, Long wordCount) {
        double doubleVal = Math.log10((double) totalDocs / (double) wordCount);
        return (double) Math.round(doubleVal * 100d) / 100d;
    }
}
