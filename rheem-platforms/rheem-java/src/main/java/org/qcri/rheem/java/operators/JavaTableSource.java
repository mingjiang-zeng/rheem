package org.qcri.rheem.java.operators;

import org.apache.commons.lang3.StringUtils;
import org.qcri.rheem.basic.operators.TableSource;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.execution.JavaExecutor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.types.RecordType;

/**
 * This is execution operator implements the {@link TableSource}.
 */
public class JavaTableSource extends TableSource implements JavaExecutionOperator {




    public JavaTableSource(String inputUrl) {

        super(inputUrl, getColumnNames(inputUrl));

    }

    public JavaTableSource(String inputUrl, String delimiter) {

        super(inputUrl,delimiter, getColumnNames(inputUrl));

    }



    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JavaTableSource(TableSource that) {
        super(that);
    }

    private static String[] getColumnNames(String url){
        String myHeader = null;
        FileSystem fs = FileSystems.getFileSystem(url).orElseThrow(
                () -> new RheemException(String.format("Cannot access file system of %s.", url))
        );

        try {
            final InputStream inputStream = fs.open(url);
            Stream<String> lines = new BufferedReader(new InputStreamReader(inputStream)).lines();
            myHeader = lines.findFirst().get();
        } catch (IOException e) {
            throw new RheemException(String.format("Reading %s failed.", url), e);
        }

        String columnNames[] = StringUtils.split(myHeader,delimiter);

       return columnNames;
    }





    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        String url = this.getInputUrl().trim();
        FileSystem fs = FileSystems.getFileSystem(url).orElseThrow(
                () -> new RheemException(String.format("Cannot access file system of %s.", url))
        );

        try {
            final InputStream inputStream = fs.open(url);
            Stream<String> lines = new BufferedReader(new InputStreamReader(inputStream)).lines();
            Stream<Record> records = lines.skip(1).map(r -> new Record(r.split(delimiter))); // skip the header line
            ((StreamChannel.Instance) outputs[0]).accept(records);
        } catch (IOException e) {
            throw new RheemException(String.format("Reading %s failed.", url), e);
        }

        ExecutionLineageNode prepareLineageNode = new ExecutionLineageNode(operatorContext);
        prepareLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "rheem.java.textfilesource.load.prepare", javaExecutor.getConfiguration()
        ));
        ExecutionLineageNode mainLineageNode = new ExecutionLineageNode(operatorContext);
        mainLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "rheem.java.textfilesource.load.main", javaExecutor.getConfiguration()
        ));

        outputs[0].getLineage().addPredecessor(mainLineageNode);

        return prepareLineageNode.collectAndMark();
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList("rheem.java.textfilesource.load.prepare", "rheem.java.textfilesource.load.main");
    }

    @Override
    public JavaTextFileSource copy() {
        return new JavaTextFileSource(this.getInputUrl());
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        throw new UnsupportedOperationException(String.format("%s does not have input channels.", this));
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

}
