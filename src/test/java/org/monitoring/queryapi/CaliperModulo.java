package org.monitoring.queryapi;

import com.google.caliper.Runner;
import com.google.caliper.SimpleBenchmark;
import java.util.Locale;

/**
 *
 * @author Michal Dubravcik
 */
public class CaliperModulo extends SimpleBenchmark {

    long num = 3546767864L;
    long step = 30000;
    
    long res;

    public void timeMod(int reps) {
        for (int i = 0; i < reps; i++) {
            for (int x = 0; x < 1000; x++) {
                long out = num - num % step;
                res = out;
            }

        }
    }

    public void timeDiv(int reps) {
        for (int i = 0; i < reps; i++) {
            for (int x = 0; x < 1000; x++) {
                long out = (num / step) * step;
                res = out;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Locale.setDefault(Locale.US);
        Runner.main(CaliperModulo.class, args);
    }
}
