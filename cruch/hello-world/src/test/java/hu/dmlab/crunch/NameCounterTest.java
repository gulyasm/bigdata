package hu.dmlab.crunch;

import static junit.framework.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.crunch.Emitter;
import org.junit.Test;

public class NameCounterTest {

    private class MockEmitter implements Emitter<String> {
        private List<String> list = new ArrayList<>();

        public MockEmitter() {
        }

        @Override
        public void emit(String arg0) {
            list.add(arg0);
        }

        @Override
        public void flush() {
            // Nothing to do
        }

        public List<String> getList() {
            return list;
        }

    }

    @Test
    public void testSplit() {
        final String example = "109993,1953-10-19,Danel,Furudate,M,1986-11-04";
        WordCountProcessor wcp = new WordCountProcessor();
        MockEmitter emitter = new MockEmitter();
        wcp.process(example, emitter);
        boolean equals = emitter.getList().equals(Arrays.asList("Danel"));
        assertTrue(equals);
    }
}
