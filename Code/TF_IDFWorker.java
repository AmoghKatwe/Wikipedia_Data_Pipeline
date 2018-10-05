package assign1;

import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.concurrent.Callable;

public class TF_IDFWorker implements Callable {
    private String keyName;

    TF_IDFWorker(String keyName) {
        //System.out.println("TF-IDF calculation starts for " + keyName);
        this.keyName = keyName;
    }

    @Override
    public Object call() throws IOException, InterruptedException {
        S3Object s3object = Main.s3ClientForWrite.getObject(Main.S3_DOC_DIR, keyName);
        S3ObjectInputStream s3is = s3object.getObjectContent();

        try (BufferedWriter bwWordFile = new BufferedWriter(new FileWriter(Main.TF_IDF_DIR + keyName))) {
            try (BufferedReader docReader = new BufferedReader(new InputStreamReader(s3is))) {
                String line;

                while ((line = docReader.readLine()) != null) {
                    String word = line.split(", ")[0];

                    try (BufferedReader idfReader = new BufferedReader(new InputStreamReader(new FileInputStream(Main.IDF_FILE)))) {
                        String idfLine;
                        while ((idfLine = idfReader.readLine()) != null) {
                            String idfWord = idfLine.split(", ")[0];
                            if (word.equals(idfWord)) {
                                Long count = Long.valueOf(line.split(", ")[1]);
                                Double tf = Double.valueOf(line.split(", ")[2]);
                                Double idf = Double.valueOf(idfLine.split(", ")[1]);

                                Double tf_idf = (double) Math.round(tf * idf * 100d) / 100d;
                                bwWordFile.write(word + ", " + count + ", " + tf + ", " + tf_idf + "\n");
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        s3is.close();

        File file = new File(Main.TF_IDF_DIR + keyName);
        Main.s3ClientForWrite.putObject(new PutObjectRequest(Main.S3_TF_IDF_DIR, keyName, file));
        FileUtils.deleteQuietly(file);
        Main.s3ClientForWrite.deleteObject(new DeleteObjectRequest(Main.S3_DOC_DIR, keyName));
        Thread.sleep(500);
        //System.out.println("TF-IDF calculation done for " + file);
        return keyName;
    }
}
