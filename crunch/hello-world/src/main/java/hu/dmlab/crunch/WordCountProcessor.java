package hu.dmlab.crunch;

import com.google.common.base.Strings;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;

public class WordCountProcessor extends DoFn<String, String> {

    private static final long serialVersionUID = 1L;

    @Override
    public void process(String arg0, Emitter<String> arg1) {
        if (!Strings.isNullOrEmpty(arg0)) {
            String[] strings = arg0.split(" ");
            for (String string : strings) {
                arg1.emit(string.toLowerCase().trim());
            }
        }
    }
}
