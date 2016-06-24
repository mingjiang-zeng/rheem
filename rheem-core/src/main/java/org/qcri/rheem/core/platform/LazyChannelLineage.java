package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.function.BiFunction;

/**
 * Keeps track of lazily executed {@link ChannelInstance}s.
 */
public class LazyChannelLineage {

    private final Node root;

    /**
     * Creates a new instance.
     *
     * @param channelInstance         the {@link ChannelInstance} being wrapped
     * @param producerOperatorContext the {@link OptimizationContext.OperatorContext} for the producing
     *                                {@link ExecutionOperator}
     * @param producerOutputIndex     the output index of the producer {@link ExecutionTask}
     */
    public LazyChannelLineage(ChannelInstance channelInstance,
                              OptimizationContext.OperatorContext producerOperatorContext,
                              int producerOutputIndex) {
        this(new Node(channelInstance, producerOperatorContext, producerOutputIndex));
    }

    private LazyChannelLineage(Node root) {
        this.root = root;
    }

    public void addPredecessor(LazyChannelLineage that) {
        this.root.add(that.root);
    }

    public <T> T traverseAndMark(T identity, BiFunction<T, Node, T> aggregator) {
        return this.root.traverse(identity, aggregator, true);
    }

    public <T> T traverse(T identity, BiFunction<T, Node, T> aggregator) {
        return this.root.traverse(identity, aggregator, false);
    }

    /**
     * Encapsulates a single {@link ChannelInstance} in a {@link LazyChannelLineage}.
     */
    public static class Node {

        private final ChannelInstance channelInstance;

        private final OptimizationContext.OperatorContext producerOperatorContext;

        private final int producerOutputIndex;

        private final Collection<Node> predecessors = new LinkedList<>();

        private Node(final ChannelInstance channelInstance,
                     final OptimizationContext.OperatorContext producerOperatorContext,
                     final int producerOutputIndex) {
            this.channelInstance = channelInstance;
            this.producerOperatorContext = producerOperatorContext;
            this.producerOutputIndex = producerOutputIndex;
        }

        private void add(Node predecessor) {
            this.predecessors.add(predecessor);
        }

        private <T> T traverse(T accumulator, BiFunction<T, Node, T> aggregator, boolean isMark) {
            if (!this.channelInstance.wasExecuted()) {
                for (Iterator<Node> i = this.predecessors.iterator(); i.hasNext(); ) {
                    Node next = i.next();
                    accumulator = next.traverse(accumulator, aggregator, isMark);
                    if (next.channelInstance.wasExecuted()) {
                        i.remove();
                    }
                }

                accumulator = aggregator.apply(accumulator, this);
                if (isMark) this.channelInstance.markExecuted();
            }

            return accumulator;
        }

        public ChannelInstance getChannelInstance() {
            return channelInstance;
        }

        public OptimizationContext.OperatorContext getProducerOperatorContext() {
            return producerOperatorContext;
        }

        public int getProducerOutputIndex() {
            return producerOutputIndex;
        }
    }

}
