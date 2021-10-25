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
package org.cloudsimplus.examples.traces.google;

import ch.qos.logback.classic.Level;
import org.cloudbus.cloudsim.brokers.DatacenterBroker;
import org.cloudbus.cloudsim.brokers.DatacenterBrokerSimple;
import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.cloudlets.CloudletSimple;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.datacenters.DatacenterSimple;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.hosts.HostSimple;
import org.cloudbus.cloudsim.resources.File;
import org.cloudbus.cloudsim.resources.Pe;
import org.cloudbus.cloudsim.resources.PeSimple;
import org.cloudbus.cloudsim.schedulers.vm.VmScheduler;
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerSpaceShared;
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.util.Conversion;
import org.cloudbus.cloudsim.util.TimeUtil;
import org.cloudbus.cloudsim.util.TraceReaderAbstract;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelDynamic;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelFull;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmSimple;
import org.cloudsimplus.builders.tables.CloudletsTableBuilder;
import org.cloudsimplus.builders.tables.TextTableColumn;
import org.cloudsimplus.traces.google.GoogleTaskEventsTraceReader;
import org.cloudsimplus.traces.google.GoogleTaskUsageTraceReader;
import org.cloudsimplus.traces.google.TaskEvent;
import org.cloudsimplus.util.Log;
import com.alibaba.fastjson.JSON;

import java.io.*;
import java.time.LocalTime;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.cloudbus.cloudsim.util.Conversion.megaBytesToBytes;
import static org.cloudbus.cloudsim.util.MathUtil.positive;

/**
 * An example showing how to create Cloudlets (tasks) from a Google Task Events
 * Trace using a {@link GoogleTaskEventsTraceReader}. Then it uses a
 * {@link GoogleTaskUsageTraceReader} to read "task usage" trace files that
 * define how the created Cloudlets will use resources along the time.
 *
 * <p>
 * The trace are located in resources/workload/google-traces/. Each line in the
 * "task events" trace defines the scheduling of tasks (Cloudlets) inside a
 * Datacenter.
 * </p>
 *
 * <p>
 * Check important details at {@link TraceReaderAbstract}. To better understand
 * the structure of trace files, check the google-cluster-data-samples.xlsx
 * spreadsheet inside the docs dir.
 * </p>
 *
 * @author Manoel Campos da Silva Filho
 * @since CloudSim Plus 4.0.0
 *
 * TODO A joint example that creates Hosts and Cloudlets from trace files will be useful.
 * TODO See https://github.com/manoelcampos/cloudsim-plus/issues/151
 * TODO {@link CloudSimTags#CLOUDLET_FAIL} events aren't been processed.
 * TODO It has to be checked how to make the Cloudlet to be executed in the
 *      Host specified in the trace file.
 */
public class GoogleTaskEventsExample1 {
    private static final String TRACE_FILENAME = "workload/google-traces/task-events-sample-1.csv";

    private static final int HOSTS = 10;
    private static final int VMS = 8;
    private static final int HOST_PES = 8;
    private static final long HOST_RAM = 2048; //in Megabytes
    private static final long HOST_BW = 10000; //in Megabits/s
    private static final long HOST_STORAGE = 1000000; //in Megabytes
    private static final double HOST_MIPS = 1000;

    /**
     * Defines a negative length for Cloudlets created from the Google Task Events Trace file
     * so that they can run indefinitely until a {@link CloudSimTags#CLOUDLET_FINISH}
     * event is received by the {@link DatacenterBroker}.
     * Check out {@link Cloudlet#setLength(long)} for details.
     */
    private static final int  CLOUDLET_LENGTH = -10_000;

    /**
        * Max number of Cloudlets to create from the Google Task Events trace file.
     * @see GoogleTaskEventsTraceReader#setMaxCloudletsToCreate(int)
     */
    private static final int MAX_CLOUDLETS = 1000000;

    private static final long VM_PES = 4;
    private static final int  VM_MIPS = 1000;
    private static final long VM_RAM = 500; //in Megabytes
    private static final long VM_BW = 100; //in Megabits/s
    private static final long VM_SIZE_MB = 1000; //in Megabytes

    private final CloudSim simulation;
    private List<DatacenterBroker> brokers;
    private Datacenter datacenter;
    private Collection<Cloudlet> cloudlets;
    private List<Host> hostList;

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        new GoogleTaskEventsExample1();
    }

    private GoogleTaskEventsExample1() throws IOException, ClassNotFoundException {
        final double startSecs = TimeUtil.currentTimeSecs();
        System.out.printf("Simulation started at %s%n%n", LocalTime.now());
        Log.setLevel(Level.TRACE);

        simulation = new CloudSim();
        datacenter = createDatacenter();
        createCloudletsAndBrokersFromTraceFile();
        readTaskUsageTraceFile();
        brokers.forEach(broker -> broker.submitVmList(createVms()));

        System.out.println("Brokers:");
        brokers.stream().sorted().forEach(b -> System.out.printf("\t%d - %s%n", b.getId(), b.getName()));
        System.out.println("Cloudlets:");
        cloudlets.stream().sorted().forEach(c -> System.out.printf("\t%s (job %d)%n", c, c.getJobId()));

        simulation.start();

        brokers.stream().sorted().forEach(this::printCloudlets);
        System.out.printf("Simulation finished at %s. Execution time: %.2f seconds%n", LocalTime.now(), TimeUtil.elapsedSeconds(startSecs));
    }

    /**
     * Creates a list of Cloudlets from a "task events" Google Cluster Data trace file.
     * The brokers that own each Cloudlet are defined by the username field
     * in the file. For each distinct username, a broker is created
     * and its name is defined based on the username.
     *
     * <p>
     * A {@link GoogleTaskEventsTraceReader} instance is used to read the file.
     * It requires a {@link Function}
     * that will be called internally to actually create the Cloudlets.
     * This function is the {@link #createCloudlet(TaskEvent)}.*
     * </p>
     *
     * <p>
     * If you want to improve simulation performance and use the same broker
     * for all created cloudlets, avoiding the reader to create brokers based on the
     * username field of the trace file, call {@link GoogleTaskEventsTraceReader#setDefaultBroker(DatacenterBroker)}.
     * </p>
     */
    private void createCloudletsAndBrokersFromTraceFile() {
        final GoogleTaskEventsTraceReader reader =
            GoogleTaskEventsTraceReader.getInstance(simulation, TRACE_FILENAME, this::createCloudlet).setMaxCloudletsToCreate(MAX_CLOUDLETS);;

        /*The created Cloudlets are automatically submitted to their respective brokers,
        so you don't have to submit them manually.*/
        cloudlets = reader.process();
        brokers = reader.getBrokers();
        System.out.printf(
            "%d Cloudlets and %d Brokers created from the %s trace file.%n",
            cloudlets.size(), brokers.size(), TRACE_FILENAME);
    }

    /**
     * A method that is used to actually create each Cloudlet defined as a task in the
     * trace file.
     * The researcher can write his/her own code inside this method to define
     * how he/she wants to create the Cloudlet based on the trace data.
     *
     * @param event an object containing the trace line read, used to create the Cloudlet.
     * @return
     */
    private Cloudlet createCloudlet(final TaskEvent event) {
        /*
        The trace doesn't define the actual number of CPU cores (PEs) a Cloudlet will require,
        but just a percentage of the number of cores that is required.
        This way, we have to compute the actual number of cores.
        This is different from the CPU UtilizationModel, which is defined
        in the "task usage" trace files.
        */
        final long pesNumber = positive(event.actualCpuCores(VM_PES), VM_PES);

        final double maxRamUsagePercent = positive(event.getResourceRequestForRam(), Conversion.HUNDRED_PERCENT);
        final UtilizationModelDynamic utilizationRam = new UtilizationModelDynamic(0, 1);

        final double sizeInMB    = event.getResourceRequestForLocalDiskSpace() * VM_SIZE_MB + 1;
        final long   sizeInBytes = (long) Math.ceil(megaBytesToBytes(sizeInMB));
        return new CloudletSimple(CLOUDLET_LENGTH, pesNumber)
            .setFileSize(sizeInBytes)
            .setOutputSize(sizeInBytes)
            .setUtilizationModelBw(new UtilizationModelFull())
            .setUtilizationModelCpu(new UtilizationModelFull())
            .setUtilizationModelRam(utilizationRam);
    }

    /**
     * Process a "task usage" trace file from the Google Cluster Data that
     * defines the resource usage for Cloudlets (tasks) along the time.
     * The reader is just considering data about RAM and CPU utilization.
     *
     * <p>
     * You are encouraged to check {@link GoogleTaskUsageTraceReader#process()}
     * documentation to understand the details.
     * </p>
     */
    private void readTaskUsageTraceFile() {
        final String fileName = "workload/google-traces/task-usage-sample-1.csv";
        final GoogleTaskUsageTraceReader reader =
            GoogleTaskUsageTraceReader.getInstance(brokers, fileName);
        final Collection<Cloudlet> processedCloudlets = reader.process();
        System.out.printf("%d Cloudlets processed from the %s trace file.%n", processedCloudlets.size(), fileName);
        System.out.println();
    }

    private Datacenter createDatacenter() {
        hostList = new ArrayList<>(HOSTS);
        for(int i = 0; i < HOSTS; i++) {
            hostList.add(createHost());
        }

        //Uses a VmAllocationPolicySimple by default
        return new DatacenterSimple(simulation, hostList);
    }

    private long getVmSize(final Cloudlet cloudlet) {
        return cloudlet.getVm().getStorage().getCapacity();
    }

    private long getCloudletSizeInMB(final Cloudlet cloudlet) {
        return (long)Conversion.bytesToMegaBytes(cloudlet.getFileSize());
    }

    private Host createHost() {
        final List<Pe> peList = createPesList(HOST_PES);
        final VmScheduler vmScheduler = new VmSchedulerTimeShared();
        //Uses a ResourceProvisionerSimple for RAM and BW
        final Host host = new HostSimple(HOST_RAM, HOST_BW, HOST_STORAGE, peList);
        host.setVmScheduler(vmScheduler);
        return host;
    }

    private List<Pe> createPesList(final int count) {
        final List<Pe> cpuCoresList = new ArrayList<>(count);
        for(int i = 0; i < count; i++){
            //Uses a PeProvisionerSimple by default
            cpuCoresList.add(new PeSimple(HOST_MIPS));
        }

        return cpuCoresList;
    }

    private List<Vm> createVms() {
        return IntStream.range(0, VMS).mapToObj(this::createVm).collect(Collectors.toList());
    }

    private Vm createVm(final int id) {
        //Uses a CloudletSchedulerTimeShared by default
        return new VmSimple(VM_MIPS, VM_PES).setRam(VM_RAM).setBw(VM_BW).setSize(VM_SIZE_MB);
    }

    private void printCloudlets(final DatacenterBroker broker) {
        final String username = broker.getName().replace("Broker_", "");
        final List<Cloudlet> list = broker.getCloudletFinishedList();
        list.sort(Comparator.comparingLong(Cloudlet::getId));
        new CloudletsTableBuilder(list)
            .addColumn(0, new TextTableColumn("Job", "ID"), Cloudlet::getJobId)
            .addColumn(7, new TextTableColumn("VM Size", "MB"), this::getVmSize)
            .addColumn(8, new TextTableColumn("Cloudlet Size", "MB"), this::getCloudletSizeInMB)
            .addColumn(10, new TextTableColumn("Waiting Time", "Seconds").setFormat("%.0f"), Cloudlet::getWaitingTime)
            .setTitle("Simulation results for Broker " + broker.getId() + " representing the username " + username)
            .build();
    }

}
