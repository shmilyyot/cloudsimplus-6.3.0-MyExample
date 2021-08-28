/*
 * CloudSim Plus: A modern, highly-extensible and easier-to-use Framework for
 * Modeling and Simulation of Cloud Computing Infrastructures and Services.
 * http://cloudsimplus.org
 *
 *     Copyright (C) 2015-2018 Universidade da Beira Interior (UBI, Portugal) and
 *     the Instituto Federal de Educação Ciência e Tecnologia do Tocantins (IFTO, Brazil).
 *
 *     This file is part of CloudSim Plus.
 *
 *     CloudSim Plus is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     CloudSim Plus is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with CloudSim Plus. If not, see <http://www.gnu.org/licenses/>.
 */
package org.cloudsimplus.traces.google;

import org.cloudbus.cloudsim.brokers.DatacenterBroker;
import org.cloudbus.cloudsim.brokers.DatacenterBrokerSimple;
import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.Simulation;
import org.cloudbus.cloudsim.core.events.CloudSimEvent;
import org.cloudbus.cloudsim.util.ResourceLoader;
import org.cloudbus.cloudsim.util.TimeUtil;
import org.cloudbus.cloudsim.util.TraceReaderAbstract;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelDynamic;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Process "task events" trace files from
 * <a href="https://github.com/google/cluster-data/blob/master/ClusterData2011_2.md">Google Cluster Data</a>
 * to create {@link Cloudlet}s belonging to cloud customers (users).
 * Customers are represented as {@link DatacenterBroker} instances created from the trace file.
 * The trace files are the ones inside the task_events sub-directory of downloaded Google traces.
 * The instructions to download the traces are provided in the link above.
 *
 * <p>The class also creates the required brokers to represent the customers (users)
 * defined by the username field inside the trace file.</p>
 *
 * <p>A spreadsheet that makes it easier to understand the structure of trace files is provided
 * in docs/google-cluster-data-samples.xlsx</p>
 *
 * <p>The documentation for fields and values were obtained from the Google Cluster trace documentation in the link above.
 * It's strongly recommended to read such a documentation before trying to use this class.</p>
 *
 * <p>Check important details at {@link TraceReaderAbstract}.</p>
 *
 * @see #process()
 *
 * @author Manoel Campos da Silva Filho
 * @since CloudSim Plus 4.0.0
 */
public class GoogleTaskEventsTraceReader extends GoogleTraceReaderAbstract<Cloudlet> {
    /** @see #getMaxCloudletsToCreate() */
    private int maxCloudletsToCreate;

    /** @see #setAutoSubmitCloudlets(boolean) */
    private boolean autoSubmitCloudlets;

    /**
     * @see #setPredicate(Predicate)
     */
    private Predicate<TaskEvent> predicate;

    /**
     * Defines a {@link Predicate} which indicates when a {@link TaskEvent}
     * must be created from a trace line read from the workload file.
     * If a Predicate is not set, a TaskEvent will be created for any line read.
     *
     * @param predicate the predicate to define when a TaskEvent must be created from a line read from the workload file
     * @return
     */
    public org.cloudsimplus.traces.google.GoogleTaskEventsTraceReader setPredicate(final Predicate<TaskEvent> predicate) {
        this.predicate = predicate;
        return this;
    }

    /**
     * Defines the type of information missing in the trace file.
     * It represents the possible values for the MISSING_INFO field.
     */
    public enum MissingInfo {
        /**
         * 0: Means Google Clusters did not find a record representing the given event,
         * but a later snapshot of the job or task state indicated that the transition must have occurred.
         * The timestamp of the synthesized event is the timestamp of the snapshot.
         */
        SNAPSHOT_BUT_NO_TRANSITION,

        /**
         * 1: Means Google Clusters did not find a record representing the given termination event,
         * but the job or task disappeared from later snapshots of cluster states,
         * so it must have been terminated.
         * The timestamp of the synthesized event is a pessimistic upper bound on its
         * actual termination time assuming it could have legitimately been missing from one snapshot.
         */
        NO_SNAPSHOT_OR_TRANSITION,

        /**
         * 2: Means Google Clusters did not find a record representing the creation of the given task or job.
         * In this case, we may be missing metadata (job name, resource requests, etc.)
         * about the job or task and we may have placed SCHEDULE or SUBMIT events latter than they actually are.
         */
        EXISTS_BUT_NO_CREATION
    }

    /**
     * The index of each field in the trace file.
     */
    public enum FieldIndex implements TraceField<GoogleTaskEventsTraceReader> {
        /**
         * 0: The index of the field containing the time the event happened (stored in microsecond
         * but converted to seconds when read from the file).
         */
        TIMESTAMP{
            /**
             * Gets the timestamp converted to seconds.
             * @param reader the reader for the trace file
             * @return
             */
            @Override
            public Double getValue(final GoogleTaskEventsTraceReader reader) {
                return TimeUtil.microToSeconds(reader.getFieldDoubleValue(this));
            }
        },

        /**
         * 1: When it seems Google Cluster is missing an event record, it's synthesized a replacement.
         * Similarly, we look for a record of every job or task that is active at the end of the trace time window,
         * and synthesize a missing record if we don't find one.
         * Synthesized records have a number (called the "missing info" field)
         * to represent why they were added to the trace, according to {@link MissingInfo} values.
         *
         * <p>When there is no info missing, the field is empty in the trace.
         * In this case, -1 is returned instead.</p>
         */
        MISSING_INFO{
            @Override
            public Integer getValue(final GoogleTaskEventsTraceReader reader) {
                return reader.getFieldIntValue(this, -1);
            }
        },

        /**
         * 2: The index of the field containing the id of the job this task belongs to.
         */
        JOB_ID{
            @Override
            public Long getValue(final GoogleTaskEventsTraceReader reader) {
                return reader.getFieldLongValue(this);
            }
        },

        /**
         * 3: The index of the field containing the task index within the job.
         */
        TASK_INDEX{
            @Override
            public Long getValue(final GoogleTaskEventsTraceReader reader) {
                return reader.getFieldLongValue(this);
            }
        },

        /**
         * 4: The index of the field containing the machineID.
         * If the field is present, indicates the machine onto which the task was scheduled,
         * otherwise, the reader will return -1 as default value.
         */
        MACHINE_ID{
            @Override
            public Long getValue(final GoogleTaskEventsTraceReader reader) {
                return reader.getFieldLongValue(this, -1);
            }
        },

        /**
         * 5: The index of the field containing the type of event.
         * The possible values for this field are the ordinal values of the enum {@link TaskEventType}.
         */
        EVENT_TYPE{
            @Override
            public Integer getValue(final GoogleTaskEventsTraceReader reader) {
                return reader.getFieldIntValue(this);
            }
        },

        /**
         * 6: The index of the field containing the hashed username provided as an opaque base64-encoded string that can be tested for equality.
         * For each distinct username, a corresponding {@link DatacenterBroker} is created.
         */
        USERNAME{
            @Override
            public String getValue(final GoogleTaskEventsTraceReader reader) {
                return reader.getFieldValue(this);
            }
        },

        /**
         * 7: All jobs and tasks have a scheduling class ​that roughly represents how latency-sensitive it is.
         * The scheduling class is represented by a single number,
         * with 3 representing a more latency-sensitive task (e.g., serving revenue-generating user requests)
         * and 0 representing a non-production task (e.g., development, non-business-critical analyses, etc.).
         * Note that scheduling  class is n​ot a priority, although more latency-sensitive tasks tend to have higher task priorities.
         * Scheduling class affects machine-local policy for resource access.
         * Priority determines whether a task is scheduled on a machine.
         *
         * <p><b>WARNING</b>: Currently, this field is totally ignored by CloudSim Plus.</p>
         */
        SCHEDULING_CLASS{
            @Override
            public Integer getValue(final GoogleTaskEventsTraceReader reader) {
                return reader.getFieldIntValue(this);
            }
        },

        /**
         * 8: Each task has a priority, a​ small integer that is mapped here into a sorted set of values,
         * with 0 as the lowest priority (least important).
         * Tasks with larger priority numbers generally get preference for resources
         * over tasks with smaller priority numbers.
         *
         * <p>There are some special priority ranges:
         * <ul>
         * <li><b>"free" priorities</b>: these are the lowest priorities.
         * Resources requested at these priorities incur little internal charging.</li>
         * <li><b>"production" priorities</b>: these are the highest priorities.
         * The cluster scheduler attempts to prevent latency-sensitive tasks at
         * these priorities from being evicted due to over-allocation of machine resources.</li>
         * <li><b>"monitoring" priorities</b>: these priorities are intended for jobs
         * which monitor the health of other, lower-priority jobs</li>
         * </ul>
         * </p>
         */
        PRIORITY{
            @Override
            public Integer getValue(final GoogleTaskEventsTraceReader reader) {
                return reader.getFieldIntValue(this);
            }
        },

        /**
         * 9: The index of the field containing the maximum number of CPU cores
         * the task is permitted to use (in percentage from 0 to 1).
         *
         * <p>When there is no value for the field, 0 is returned instead.</p>
         */
        RESOURCE_REQUEST_FOR_CPU_CORES{
            @Override
            public Double getValue(final GoogleTaskEventsTraceReader reader) {
                return reader.getFieldDoubleValue(this, 0);
            }
        },

        /**
         * 10: The index of the field containing the maximum amount of RAM
         * the task is permitted to use (in percentage from 0 to 1).
         *
         * <p>When there is no value for the field, 0 is returned instead.</p>
         */
        RESOURCE_REQUEST_FOR_RAM{
            @Override
            public Double getValue(final GoogleTaskEventsTraceReader reader) {
                return reader.getFieldDoubleValue(this, 0);
            }
        },

        /**
         * 11: The index of the field containing the maximum amount of local disk space
         * the task is permitted to use (in percentage from 0 to 1).
         *
         * <p>When there is no value for the field, 0 is returned instead.</p>
         */
        RESOURCE_REQUEST_FOR_LOCAL_DISK_SPACE{
            @Override
            public Double getValue(final GoogleTaskEventsTraceReader reader) {
                return reader.getFieldDoubleValue(this, 0);
            }
        },

        /**
         * 12: If the different-machine constraint​ field is present, and true (1),
         * it indicates that a task must be scheduled to execute on a
         * different machine than any other currently running task in the job.
         * It is a special type of constraint.
         *
         * <p>When there is no value for the field, -1 is returned instead.</p>
         */
        DIFFERENT_MACHINE_CONSTRAINT{
            @Override
            public Integer getValue(final GoogleTaskEventsTraceReader reader) {
                return reader.getFieldIntValue(this, -1);
            }
        }
    }

    /**
     * List of messages to send to the {@link DatacenterBroker} that owns each created Cloudlet.
     * Such events request a Cloudlet's status change or attributes change.
     * @see #addCloudletStatusChangeEvents(CloudSimEvent, TaskEvent)
     */
    protected final Map<Cloudlet, List<CloudSimEvent>> cloudletEvents;

    /**
     * @see #setCloudletCreationFunction(Function)
     */
    private Function<TaskEvent, Cloudlet> cloudletCreationFunction;

    /**
     * A default broker to be used by all created Cloudlets.
     * @see #setDefaultBroker(DatacenterBroker)
     * @see #brokersMap
     */
    private DatacenterBroker defaultBroker;

    /**
     * A map of brokers created according to the username from the trace file,
     * representing a customer. Each key is the username field and the value the created broker.
     * If a default {@link #defaultBroker} is set, the map is empty.
     * @see #getBrokers()
     */
    private final Map<String, DatacenterBroker> brokersMap;

    private final CloudSim simulation;

    /**
     * Gets a {@link GoogleTaskEventsTraceReader} instance to read a "task events" trace file
     * inside the <b>application's resource directory</b>.
     *
     * @param simulation the simulation instance that the created tasks and brokers will belong to.
     * @param filePath the workload trace <b>relative file name</b> in one of the following formats: <i>ASCII text, zip, gz.</i>
     * @param cloudletCreationFunction A {@link Function} that will be called for every {@link Cloudlet} to be created
     *                               from a line inside the trace file.
     *                               The {@link Function} will receive a {@link TaskEvent} object containing
     *                               the task data read from the trace and must return a new Cloudlet
     *                               according to such data.
     * @throws IllegalArgumentException when the trace file name is null or empty
     * @throws UncheckedIOException     when the file cannot be accessed (such as when it doesn't exist)
     * @see #process()
     */
    public static GoogleTaskEventsTraceReader getInstance(
        final CloudSim simulation,
        final String filePath,
        final Function<TaskEvent, Cloudlet> cloudletCreationFunction)
    {
        final InputStream reader = ResourceLoader.newInputStream(filePath, GoogleTaskEventsTraceReader.class);
        return new GoogleTaskEventsTraceReader(simulation, filePath, reader, cloudletCreationFunction);
    }

    /**
     * Instantiates a {@link GoogleTaskEventsTraceReader} to read a "task events" file.
     *
     * @param simulation the simulation instance that the created tasks and brokers will belong to.
     * @param filePath               the workload trace <b>relative file name</b> in one of the following formats: <i>ASCII text, zip, gz.</i>
     * @param cloudletCreationFunction A {@link Function} that will be called for every {@link Cloudlet} to be created
     *                               from a line inside the trace file.
     *                               The {@link Function} will receive a {@link TaskEvent} object containing
     *                               the task data read from the trace and must return a new Cloudlet
     *                               according to such data.
     * @throws IllegalArgumentException when the trace file name is null or empty
     * @throws UncheckedIOException     when the file cannot be accessed (such as when it doesn't exist)
     * @see #process()
     */
    public GoogleTaskEventsTraceReader(
        final CloudSim simulation,
        final String filePath,
        final Function<TaskEvent, Cloudlet> cloudletCreationFunction) throws IOException
    {
        this(simulation, filePath, Files.newInputStream(Paths.get(filePath)), cloudletCreationFunction);
    }

    /**
     * Instantiates a {@link GoogleTaskEventsTraceReader} to read a "task events" from a given InputStream.
     *
     * @param simulation the simulation instance that the created tasks and brokers will belong to.
     * @param filePath               the workload trace <b>relative file name</b> in one of the following formats: <i>ASCII text, zip, gz.</i>
     * @param reader                 a {@link InputStream} object to read the file
     * @param cloudletCreationFunction A {@link Function} that will be called for every {@link Cloudlet} to be created
     *                               from a line inside the trace file.
     *                               The {@link Function} will receive a {@link TaskEvent} object containing
     *                               the task data read from the trace and must return a new Cloudlet
     *                               according to such data.
     * @throws IllegalArgumentException when the trace file name is null or empty
     * @throws UncheckedIOException     when the file cannot be accessed (such as when it doesn't exist)
     * @see #process()
     */
    protected GoogleTaskEventsTraceReader(
        final CloudSim simulation,
        final String filePath,
        final InputStream reader,
        final Function<TaskEvent, Cloudlet> cloudletCreationFunction)
    {
        super(filePath, reader);
        this.simulation = requireNonNull(simulation);
        this.cloudletCreationFunction = requireNonNull(cloudletCreationFunction);
        this.autoSubmitCloudlets = true;
        brokersMap = new HashMap<>();
        cloudletEvents = new HashMap<>();
        setMaxCloudletsToCreate(Integer.MAX_VALUE);
        /*
        A default predicate which indicates that a Cloudlet will be
        created for any job read from the workload reader.
        That is, there isn't an actual condition to create a Cloudlet.
        */
        this.predicate = cloudlet ->true;
    }

    /**
     * Process the {@link #getFilePath() trace file} creating a Set of {@link Cloudlet}s
     * described in the file. <b>Each created Cloudlet is automatically submitted to its respective
     * broker.</b>
     *
     * <p>It returns the Set of all submitted {@link Cloudlet}s at any timestamp inside the trace file
     * (the timestamp is used to delay the Cloudlet submission).
     * </p>
     *
     * @return the Set of all submitted {@link Cloudlet}s for any timestamp inside the trace file.
     * @see #getBrokers()
     */
    @Override
    public final Collection<Cloudlet> process() {
        if(!autoSubmitCloudlets) {
            LOGGER.info("{}: Auto-submission of Cloudlets from trace file is disabled. Don't forget to submitted them to the broker.", getClass().getSimpleName());
        }

        return super.process();
    }

    /**
     * There is no pre-process requirements for this implementation.
     */
    @Override
    protected void preProcess(){/**/}

    @Override
    protected void postProcess(){
        if(simulation.isRunning())
            sendCloudletEvents();
        else simulation.addOnSimulationStartListener(info -> sendCloudletEvents());
    }

    private void sendCloudletEvents() {
        cloudletEvents.values().forEach(this::sendCloudletEvents);
    }

    protected void sendCloudletEvents(final List<CloudSimEvent> events) {
        events.forEach(evt -> evt.getSource().schedule(evt));
    }

    @Override
    protected boolean processParsedLineInternal() {
        return getEventType().process(this,predicate);
    }

    /**
     * Gets the enum value that represents the event type of the current trace line.
     *
     * @return the {@link MachineEventType} value
     */
    private TaskEventType getEventType() {
        return TaskEventType.getValue(FieldIndex.EVENT_TYPE.getValue(this));
    }

    protected TaskEvent createTaskEventFromTraceLine() {
        final TaskEvent event = new TaskEvent();
        /*@TODO The tasks with the same username must run inside the same user's VM,
        *       unless the machineID is different.
        *       The task (cloudlet) needs to be mapped to a specific Host (according to the machineID).
        *       The challenge here is because the task requirements are usually not known,
        *       for instance when the task is submitted. It's just know when it starts to execute.
        */
        event
            .setType(FieldIndex.EVENT_TYPE.getValue(this))
            .setTimestamp(FieldIndex.TIMESTAMP.getValue(this))
            .setResourceRequestForCpuCores(FieldIndex.RESOURCE_REQUEST_FOR_CPU_CORES.getValue(this))
            .setResourceRequestForLocalDiskSpace(FieldIndex.RESOURCE_REQUEST_FOR_LOCAL_DISK_SPACE.getValue(this))
            .setResourceRequestForRam(FieldIndex.RESOURCE_REQUEST_FOR_RAM.getValue(this))
            .setPriority(FieldIndex.PRIORITY.getValue(this))
            .setSchedulingClass(FieldIndex.SCHEDULING_CLASS.getValue(this))
            .setUserName(FieldIndex.USERNAME.getValue(this))
            .setJobId(FieldIndex.JOB_ID.getValue(this))
            .setTaskIndex(FieldIndex.TASK_INDEX.getValue(this))
            .setMachineId(org.cloudsimplus.traces.google.GoogleTaskEventsTraceReader.FieldIndex.MACHINE_ID.getValue(this)); //Eason定制修改，因为task在那里执行会变化，所以默认没有指定machineId
        return event;
    }

    /**
     * Send a message to the broker to request change in a Cloudlet status,
     * using some tags from {@link CloudSimTags} such as {@link CloudSimTags#CLOUDLET_READY}.
     * @param tag a CLOUDLET tag from the {@link CloudSimTags} used to send a message to request the Cloudlet status change
     * @return true if the request was created, false otherwise
     */
    /* default */ boolean requestCloudletStatusChange(final int tag) {
        final TaskEvent taskEvent = createTaskEventFromTraceLine();
        final DatacenterBroker broker = getBroker(taskEvent.getUserName());
        final double delay = taskEvent.getTimestamp();

        return findObject(taskEvent.getUniqueTaskId())
                .map(cloudlet -> addCloudletStatusChangeEvents(new CloudSimEvent(delay, broker, tag, cloudlet), taskEvent))
                .isPresent();
    }

    /**
     * Adds the events to request to change the status and attributes of a Cloudlet to the
     * list of events to send to the Cloudlet's broker.
     *
     * @param statusChangeSimEvt the {@link CloudSimEvent} to be sent requesting the change in a Cloudlet's status,
     *                           where the data of the event is the Cloudlet to be changed.
     * @param taskEvent the task event read from the trace file, containing
     *                  the status and the attributes to change in the Cloudlet
     * @return
     * @TODO This method is too large and confusing, thus it needs to be refactored
     */
    private Cloudlet addCloudletStatusChangeEvents(final CloudSimEvent statusChangeSimEvt, final TaskEvent taskEvent){
        /*The actual Cloudlet that needs to have its status and/or attributes changed
         * by sending a request message to the broker.*/
        final Cloudlet cloudlet = (Cloudlet)statusChangeSimEvt.getData();

        addEventToSend(cloudlet, statusChangeSimEvt);

        /*
        Creates a temporary Cloudlet that will be discharged after the method finish.
        It is used just because the "cloudlet creation function" provided
        by the researcher is the only one that actually knows how to create a Cloudlet
        from the trace line.
        The TaskEvent object contains the data read from a trace line,
        but some Cloudlet attributes such as RAM and CPU UtilizationModel
        are instantiated based on a class chosen by the researcher.
        The UtilizationModel instance will just use the data read
        from the trace.
        This way, the temporary Cloudlet is created using the researcher's
        provided function so that the correct objects for such attributes
        are created and then set to the Cloudlet being updated.
         */
        final Cloudlet clone = createCloudlet(taskEvent);
        clone.setId(cloudlet.getId());

        /* If some attribute of the Cloudlet that needs to be changed,
         * sends a message requesting the change.*/
        if(!areCloudletAttributesDifferent(cloudlet, clone)){
            return cloudlet;
        }

        /* Defines a Runnable that will be executed when
         * the message is processed by the broker to update the Cloudlet attributes.
         * The "resource request" fields define the max amount of a resource the Cloudlet
         * is allowed to used and they are just used to define
         * initial number of CPUs, RAM utilization and Cloudlet file size/output size.
         * This way, when they change along the time in the trace,
         * these new values are ignored because
         */
        final Runnable attributesUpdateRunnable = () -> {
            final DatacenterBroker broker = cloudlet.getBroker();
            final StringBuilder builder = new StringBuilder();
            /* The output size doesn't always have a relation with file size.
             * This way, if the file size is changed, we don't change
             * the output size. This may be performed by the researcher if he/she needs.*/
            if(clone.getPriority() != cloudlet.getPriority()){
                builder.append("priority: ")
                    .append(cloudlet.getPriority()).append(VAL_SEPARATOR)
                    .append(clone.getPriority()).append(COL_SEPARATOR);
                cloudlet.setFileSize(clone.getFileSize());
            }

            if(clone.getNumberOfPes() != cloudlet.getNumberOfPes()){
                builder.append("PEs: ")
                    .append(cloudlet.getNumberOfPes()).append(VAL_SEPARATOR)
                    .append(clone.getNumberOfPes()).append(COL_SEPARATOR);
                cloudlet.setNumberOfPes(clone.getNumberOfPes());
            }

            //It's ensured when creating the Cloudlet that a UtilizationModelDynamic is used for RAM
            final UtilizationModelDynamic cloneRamUM = (UtilizationModelDynamic)clone.getUtilizationModelRam();
            final UtilizationModelDynamic cloudletRamUM = (UtilizationModelDynamic)cloudlet.getUtilizationModelRam();
            if(cloneRamUM.getMaxResourceUtilization() != cloudletRamUM.getMaxResourceUtilization()){
                builder.append("Max RAM Usage: ")
                    .append(formatPercentValue(cloudletRamUM.getMaxResourceUtilization())).append(VAL_SEPARATOR)
                    .append(formatPercentValue(cloneRamUM.getMaxResourceUtilization())).append("% | ");
                cloudletRamUM.setMaxResourceUtilization(cloneRamUM.getMaxResourceUtilization());
            }

            /* We don't need to check if some Cloudlet attribute was changed because
             * if this Runnable is executed is because something was.
             * An event to execute such Runnable is just sent in such a condition.*/
            broker.LOGGER.trace("{}: {}: {} attributes updated: {}", getSimulation().clockStr(), broker.getName(), cloudlet, builder);
        };

        /* The Runnable is the data of the event that is sent to the broker.
         * This way, it will be executed only when the event is processed.*/
        final CloudSimEvent attrsChangeSimEvt =
            new CloudSimEvent(
                taskEvent.getTimestamp(),
                statusChangeSimEvt.getDestination(),
                CloudSimTags.CLOUDLET_UPDATE_ATTRIBUTES, attributesUpdateRunnable);

        //Sends the event to change the Cloudlet attributes
        addEventToSend(cloudlet, attrsChangeSimEvt);

        return cloudlet;
    }

    /**
     * Adds a Cloudlet event to the map of events to send
     * @param cloudlet
     * @param evt
     */
    private void addEventToSend(final Cloudlet cloudlet, final CloudSimEvent evt) {
        cloudletEvents
            .compute(cloudlet, (key, list) -> list == null ? new LinkedList<>() : list)
            .add(evt);
    }

    protected Cloudlet createCloudlet(final TaskEvent taskEvent) {
        final Cloudlet cloudlet = cloudletCreationFunction.apply(taskEvent);
        if(cloudlet.getUtilizationModelRam() instanceof UtilizationModelDynamic) {
            return cloudlet;
        }

        throw new IllegalStateException(
            "Since the 'task events' trace file provides a field defining the max RAM the Cloudlet can request (RESOURCE_REQUEST_FOR_RAM), " +
                "it's required to use a UtilizationModelDynamic for the Cloudlet's RAM utilization model, " +
                "so that the UtilizationModelDynamic.maxResourceUtilization attribute can be changed when defined in the trace file.");

    }

    private boolean areCloudletAttributesDifferent(final Cloudlet cloudlet1, final Cloudlet cloudlet2) {
        return cloudlet2.getFileSize() != cloudlet1.getFileSize() ||
               cloudlet2.getNumberOfPes() != cloudlet1.getNumberOfPes() ||
               cloudlet2.getUtilizationOfCpu() != cloudlet1.getUtilizationOfCpu() ||
               cloudlet2.getUtilizationOfRam() != cloudlet1.getUtilizationOfRam();
    }

    /**
     * Gets a {@link Function} that will be called for every {@link Cloudlet} to be created
     * from a line inside the trace file.
     * @return
     * @see #setCloudletCreationFunction(Function)
     */
    protected Function<TaskEvent, Cloudlet> getCloudletCreationFunction() {
        return cloudletCreationFunction;
    }

    /**
     * Sets a {@link Function} that will be called for every {@link Cloudlet} to be created
     * from a line inside the trace file.
     * The {@link Function} will receive a {@link TaskEvent} object containing
     * the task data read from the trace and should the created Cloudlet.
     * The provided function must instantiate the Host and defines Host's CPU cores and RAM
     * capacity according the the received parameters.
     * For other Hosts configurations (such as storage capacity), the provided
     * function must define the value as desired, since the trace file
     * doesn't have any other information for such resources.
     *
     * @param cloudletCreationFunction the {@link Function} to set
     */
    public void setCloudletCreationFunction(final Function<TaskEvent, Cloudlet> cloudletCreationFunction) {
        this.cloudletCreationFunction = requireNonNull(cloudletCreationFunction);
    }

    /**
     * Gets the List of brokers created according to the username from the trace file,
     * representing a customer.
     * @return
     * @see #setDefaultBroker(DatacenterBroker)
     */
    public List<DatacenterBroker> getBrokers() {
        return defaultBroker == null ? new ArrayList<>(brokersMap.values()) : Collections.singletonList(defaultBroker);
    }

    /**
     * Defines a default broker to will be used for all created Cloudlets.
     * This way, the username field inside the trace file won't be used
     * to dynamically create brokers.
     * The {@link #getBrokers()} will only return an unitary list containing this broker.
     *
     * @param broker the broker for all created cloudlets, representing a single username (customer)
     * @return
     */
    public GoogleTaskEventsTraceReader setDefaultBroker(final DatacenterBroker broker) {
        this.defaultBroker = Objects.requireNonNull(broker);
        brokersMap.clear();
        return this;
    }

    /**
     * Gets a {@link #setDefaultBroker(DatacenterBroker) default broker (if ones was set)}
     * or the one with the specified username (creating it if not yet).
     * @param username the username of the broker
     * @return (i) an already existing broker with the given username or a new one if not created yet;
     *         (ii) the default broker if one was set.
     */
    protected DatacenterBroker getOrCreateBroker(final String username){
        return getBroker(() -> brokersMap.computeIfAbsent(username, this::createBroker));
    }

    private DatacenterBroker createBroker(final String username) {
        return new DatacenterBrokerSimple(simulation, "Broker_"+username);
    }

    /**
     * Gets an {@link DatacenterBroker} instance represented by a given username.
     * If a {@link #setDefaultBroker(DatacenterBroker) default broker was set to be used for all created Cloudlets},
     * that one is returned, ignoring the username given.
     *
     * @param username the name of the user read from a trace line
     * @return the {@link DatacenterBroker} instance for the given username or the default broker (if it was set)
     */
    private DatacenterBroker getBroker(final String username){
        return getBroker(() -> brokersMap.get(username));
    }

    /**
     * Gets the default broker or the one returned by the provided supplier
     * @param supplier a broker {@link Supplier} Function.
     * @return
     */
    private DatacenterBroker getBroker(final Supplier<DatacenterBroker> supplier){
        return defaultBroker == null ? supplier.get() : defaultBroker;
    }

    /**
     * Gets an {@link DatacenterBroker} instance representing the username from the last trace line read.
     * @return the {@link DatacenterBroker} instance
     */
    protected DatacenterBroker getBroker(){
        final String value = FieldIndex.USERNAME.getValue(this);
        return getBroker(value);
    }

    public Simulation getSimulation() {
        return simulation;
    }

    /**
     * Gets the maximum number of Cloudlets to create from the trace file.
     * @return
     * @see #setMaxCloudletsToCreate(int)
     */
    public int getMaxCloudletsToCreate() {
        return maxCloudletsToCreate;
    }

    /**
     * Sets the maximum number of Cloudlets to create from the trace file.
     * Since the number of lines in the file may be greater to the number
     * of Cloudlets that will be created from it (since there may be lines
     * representing different cloudlet requests, such as creation, execution, pause, destruction, etc),
     * using the {@link #setMaxLinesToRead(int)} may not ensure only a given number of
     * Cloudlets will be created.
     * @param maxCloudletsToCreate the maximum number of Cloudlets to create from the file.
     *                             Use {@link Integer#MAX_VALUE} to disable this configuration.
     * @return
     * @see #setMaxLinesToRead(int)
     */
    public GoogleTaskEventsTraceReader setMaxCloudletsToCreate(final int maxCloudletsToCreate) {
        if(maxCloudletsToCreate <= 0) {
            throw new IllegalArgumentException("Maximum number of Cloudlets to create must be greater than 0. If you want to create all Cloudlets from the entire file, provide Integer.MAX_VALUE.");
        }

        this.maxCloudletsToCreate = maxCloudletsToCreate;
        return this;
    }

    /**
     * Checks if the maximum number of Cloudlets to create was not reached.
     * @return true to indicate the Cloudlet is allowed to be created, false otherwise.
     */
    protected boolean allowCloudletCreation() {
        return availableObjectsCount() < getMaxCloudletsToCreate();
    }

    /**
     * Checks if Cloudlets will be auto-submitted to the broker after created
     * (default is true).
     * @return true if auto-submit is enabled, false otherwise
     */
    public boolean isAutoSubmitCloudlets() {
        return autoSubmitCloudlets;
    }

    /**
     * Sets if Cloudlets must be auto-submitted to the broker after created
     * (default is true).
     * @param autoSubmitCloudlets true to auto-submit, false otherwise
     */
    public GoogleTaskEventsTraceReader setAutoSubmitCloudlets(final boolean autoSubmitCloudlets) {
        this.autoSubmitCloudlets = autoSubmitCloudlets;
        return this;
    }
}
