package com.mypackage.pipeline;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.MatchResult;
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

    static class MoveFilesUp extends DoFn<MatchResult.Metadata, String> {
    
        @ProcessElement
        public void processElement(ProcessContext c) {
            MatchResult.Metadata metadata = c.element();
            String filePath = metadata.resourceId().toString();
            String[] levels = filePath.split("/");
            if (levels.length >= 2 && levels[levels.length - 2].equals(levels[levels.length - 3])) {
                LOG.info("Matched directories: {} and {}", levels[levels.length - 2], levels[levels.length - 3]);
                c.output(filePath);
            }
        }
    }

    static class MoveFileFn extends DoFn<String, Void> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String filePath = c.element();
            String directoryPath = filePath.substring(0, filePath.lastIndexOf("/"));
            String paretDirectoryPath = directoryPath.substring(0, directoryPath.lastIndexOf("/"));

            String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);
            String destinationPath = paretDirectoryPath + "/" + fileName;

            String gsutilCommand = "gsutil mv " + filePath + " " + destinationPath;
            LOG.info("Moving file from {} to {}", filePath, destinationPath);
            try {
                Process process = Runtime.getRuntime().exec(gsutilCommand);
                int exitCode = process.waitFor();
                if (exitCode != 0) {
                    throw new RuntimeException("Failed to move file: " + filePath);
                }
                LOG.info("File moved successfully: {}", filePath);
                
                String gsutilRemoveCommand = "gsutil rm " + directoryPath;
                LOG.info("Removing empty folder: {}", directoryPath);
                Process removeProcess = Runtime.getRuntime().exec(gsutilRemoveCommand);
                int removeExitCode = removeProcess.waitFor();
                if (removeExitCode != 0) {
                    LOG.warn("Failed to remove empty folder: {}", directoryPath);
                } else {
                    LOG.info("Empty folder removed: {}", directoryPath);
                }
            } catch (Exception e) {
                throw new RuntimeException("Error executing gsutil command: " + gsutilCommand, e);
            }
        }
    }

    public static PipelineResult run(Options options) {

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);
        options.setJobName("my-pipeline-" + System.currentTimeMillis());

        // input and output
        String inputGcsPath = "gs://dataflow-poc-divya/input/aa/bb/bs=2023-06-28/bs=2023-06-28/*";

        pipeline
            .apply("Match Files", FileIO.match().filepattern(inputGcsPath))
            .apply("Check Last Two Directories", ParDo.of(new MoveFilesUp()))
            .apply("Move Files", ParDo.of(new MoveFileFn()));
        LOG.info("Building pipeline...");

        return pipeline.run();
    }
}
