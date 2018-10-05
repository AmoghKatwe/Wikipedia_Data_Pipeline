package assign1;

import java.util.concurrent.Callable;

public class IDFWorker implements Callable {
    private long noOfDocsInTotal;
    private String word;

    IDFWorker (long noOfDocsInTotal, String word) {
        this.noOfDocsInTotal = noOfDocsInTotal;
        this.word = word;
    }

    @Override
    public Object call() {
        //System.out.println("Calculating IDF for ***" + word + "***");
        DocInfo.calculateIDF(noOfDocsInTotal, word);
        //System.out.println("Calculated IDF for ***" + word + "***");
        return word;
    }
}
