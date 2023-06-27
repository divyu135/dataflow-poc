package com.mypackage.pipeline;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MyPipeline {


    private static final Logger LOG = LoggerFactory.getLogger(MyPipeline.class);
    public interface Options extends DataflowPipelineOptions {}

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).as(Options.class);
        run(options);
    }

    static class CheckNestedDirectory extends DoFn<String, String> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            String path = c.element();
            String[] levels = path.split("/");
            
            if (levels.length >= 2){
                String lastLLevel = levels[levels.length - 1]
                String secondLastLevel = levels[levels.length -2]
                if (lastLLevel.equals(secondLastLevel)){
                    c.output(path);
                }
            } 
           
        }
    }

    public static PipelineResult run(Options options) {

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);
        options.setJobName("my-pipeline-" + System.currentTimeMillis());

        // input and output
        String input = "gs://dataflow-poc-divya/input/test-input.txt";
        String output = "gs://dataflow-poc-divya/output/test-output.txt";

        pipeline.apply("ReadFromGCS", TextIO.read().from(input))
                .apply("ParseFilePaths", ParDo.of(new CheckNestedDirectory()))
                .apply("Write Output", TextIO.write().to(output));
        LOG.info("Building pipeline...");

        return pipeline.run();
    }
}