package hu.dmlab.crunch;

import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.writable.Writables;

public class WordCount {

    public static void main(String[] args) throws Exception {
        new WordCount().run(args);
    }

    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Error. Not enough parameters.");
        }
        String inputPath = args[0];
        String outputPath = args[1];

        // Create an object to coordinate pipeline creation and execution.
        Pipeline pipeline = MemPipeline.getInstance();
        PCollection<String> data = pipeline.readTextFile(inputPath);
        data = data.parallelDo(new WordCountProcessor(), Writables.strings());
        data = data.filter(new NameFilter());
        PTable<String, Long> pTable = data.count();
        // We can easily limit the result to the top ten elements.
        pipeline.writeTextFile(pTable.top(20), outputPath);
        PipelineResult result = pipeline.done();
        if (!result.succeeded()) {
            throw new RuntimeException("Pipeline exited with error.");
        }
        return 0;
    }
}
