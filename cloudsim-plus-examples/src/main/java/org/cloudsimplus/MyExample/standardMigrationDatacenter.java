package org.cloudsimplus.MyExample;

import ch.qos.logback.classic.Level;
import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicy;
import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigrationBestFitStaticThreshold;
import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigrationStaticThreshold;
import org.cloudbus.cloudsim.brokers.DatacenterBroker;
import org.cloudbus.cloudsim.brokers.DatacenterBrokerSimple;
import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.cloudlets.CloudletSimple;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.datacenters.DatacenterSimple;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.hosts.HostSimple;
import org.cloudbus.cloudsim.power.PowerMeter;
import org.cloudbus.cloudsim.power.models.PowerModelHost;
import org.cloudbus.cloudsim.power.models.PowerModelHostSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.ResourceProvisionerSimple;
import org.cloudbus.cloudsim.resources.Pe;
import org.cloudbus.cloudsim.resources.PeSimple;
import org.cloudbus.cloudsim.schedulers.MipsShare;
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.selectionpolicies.VmSelectionPolicyMinimumUtilization;
import org.cloudbus.cloudsim.util.Conversion;
import org.cloudbus.cloudsim.util.TimeUtil;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModel;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelDynamic;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmSimple;
import org.cloudsimplus.listeners.DatacenterBrokerEventInfo;
import org.cloudsimplus.listeners.EventInfo;
import org.cloudsimplus.listeners.EventListener;
import org.cloudsimplus.listeners.VmHostEventInfo;
import org.cloudsimplus.traces.google.*;
import org.cloudsimplus.util.Log;
import java.io.*;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import static org.cloudbus.cloudsim.util.Conversion.megaBytesToBytes;
import static org.cloudbus.cloudsim.util.MathUtil.positive;

/**
 * ??????????????????????????????????????????
 * ???????????????????????????????????????????????????????????????{@link Constant}?????????????????????????????????????????????
 * ?????????cloudlet??????????????????vm???cpu?????????????????????????????????????????????????????????
 * TODO??? ????????????SLA????????????
 * */

public class standardMigrationDatacenter {

    /**
     * cloudsim??????????????????????????????
     * */
    private final CloudSim simulation;  //??????????????????
    private List<Datacenter> datacenters;   //??????????????????
    private Collection<Cloudlet> cloudlets;    //?????????????????????
    private List<Host> hostList;    //????????????
    private List<DatacenterBroker> brokers;    //???????????????????????????
    private final List<Vm> vmList = new ArrayList<>();    //???????????????
    private DatacenterBroker broker;    //???????????????????????????
    private Set<Long> hostIds;  //????????????host???id
    private Set<Long> cloudletIds;  //?????????cloudlet???id
    public static GoogleTraceHandler googleTraceHandler;  //???????????????????????????
    public static serialObject serialObjectHandler;   //????????????????????????
    public static DataCenterPrinter dataCenterPrinter;      //??????????????????????????????
    private double lastClockTime;   //?????????????????????
    private VmAllocationPolicyMigrationStaticThreshold allocationPolicy;    //????????????
    private int migrationsNumber = 0;   //????????????
    private boolean npa = false;

    public static void main(String[] args) throws IOException, ClassNotFoundException {

        //?????????????????????????????????log
        if(Constant.PRINT_LOCAL_LOG){
            try{
                System.setOut(new PrintStream(new FileOutputStream(Constant.LOG_FILE_PATH)));
            }catch (FileNotFoundException e){
                e.printStackTrace();
            }
        }

        //????????????????????????
        serialObjectHandler = new serialObject();
        //??????????????????????????????
        googleTraceHandler = new GoogleTraceHandler();
        //??????????????????????????????
        dataCenterPrinter = new DataCenterPrinter();
        //????????????????????????
        googleTraceHandler.buildTraceFileNamesSample();

        //????????????????????????
        new standardMigrationDatacenter();
    }

    private standardMigrationDatacenter() throws IOException, ClassNotFoundException {

        //???????????????????????????????????????
        final double startSecs = TimeUtil.currentTimeSecs();
        System.out.printf("Simulation started at %s%n%n", LocalTime.now());
        Log.setLevel(DatacenterBroker.LOGGER,Level.TRACE);

        //??????????????????
        simulation = new CloudSim();

        //????????????????????????????????????????????????????????????????????????
        if(Constant.USING_GOOGLE_HOST){
            createGoogleDatacenters();
        }else{
            createModifyDatacenters();
            hostList.forEach(host-> hostIds.add(host.getId()));
        }

        //???Google????????????????????????????????????cloudlet??????
        createCloudletsAndBrokersFromTraceFileType1();

        //???GoogleUsageTrace???????????????Cloudlet????????????
        readTaskUsageTraceFile();

        //??????vm???????????????cloudlet
        //???????????????????????????????????????
        brokers.forEach(this::createAndSubmitVms);
//        brokers.forEach(broker->broker.setFailedVmsRetryDelay(-1));

        //??????brokers???cloudlets?????????
        System.out.println("Brokers:");
        brokers.stream().sorted().forEach(b -> System.out.printf("\t%d - %s%n", b.getId(), b.getName()));
        System.out.println("Cloudlets:");
        cloudlets.stream().sorted().forEach(c -> System.out.printf("\t%s (job %d)%n", c, c.getJobId()));

//        //????????????????????????
//        simulation.addOnClockTickListener(this::clockTickListener);
        //???????????????????????????
        brokers.forEach(broker -> broker.addOnVmsCreatedListener(this::onVmsCreatedListener));

        //????????????????????????????????????
        //?????????????????????????????????
        PowerMeter powerMeter = new PowerMeter(simulation, datacenters);

        //???????????????????????????
        simulation.start();

        //????????????cloudlet????????????
        brokers.stream().sorted().forEach(broker->dataCenterPrinter.printCloudlets(broker));

//        //???????????????????????????????????????
//        hostList.stream().forEach(this::printHostInfo);

//        //??????host???cpu?????????
//        System.out.printf("%nHosts CPU usage History (when the allocated MIPS is lower than the requested, it is due to VM migration overhead)%n");
//        hostList.forEach(host->dataCenterPrinter.printHostStateHistory(host));

//        //????????????
//        dataCenterPrinter.printVmsCpuUtilizationAndPowerConsumption(brokers);
//        dataCenterPrinter.printHostsCpuUtilizationAndPowerConsumption(hostList);

        //?????????????????????????????????
        dataCenterPrinter.printDataCenterTotalEnergyComsumption(powerMeter,npa);

        //??????????????????
        System.out.printf("Number of VM migrations: %d%n", migrationsNumber);

        //??????????????????
        final double endSecs = TimeUtil.currentTimeSecs();
        System.out.printf("Simulation finished at %s. Execution time: %.2f seconds%n", LocalTime.now(), TimeUtil.elapsedSeconds(startSecs));
    }

    private void createCloudletsAndBrokersFromTraceFileType1() throws IOException, ClassNotFoundException {
        cloudlets = new HashSet<>(30000);
        cloudletIds = new HashSet<>(30000);
        brokers = new ArrayList<>(100000);

        //??????????????????
        if(Constant.SINGLE_BROKER){
            broker = new DatacenterBrokerSimple(simulation);
            brokers.add(broker);
        }

        //???????????????cloudlet id
        Set<Long> finalEligibleCloudletIds;
        if(Constant.USING_EXISTANCE_PRECLOULETS){
            //???????????????????????????n?????????event????????????java heap?????????????????????????????????????????????usage????????????
            Set<Long> eligibleCloudletIds;
            //??????????????????????????????????????????usage?????????????????????????????????cloudlet id????????????????????????????????????id????????????cloudlet
            if(serialObjectHandler.checkObjectExist(Constant.SERIAL_PRECLOUDLETID_PATH)){
                eligibleCloudletIds = serialObjectHandler.reverseSerializableObject(Constant.SERIAL_PRECLOUDLETID_PATH);
            }else{
                eligibleCloudletIds = googleTraceHandler.filterCloudletsUsageIs(simulation);
                serialObjectHandler.serializableObject(eligibleCloudletIds,Constant.SERIAL_PRECLOUDLETID_PATH);
            }
            finalEligibleCloudletIds = eligibleCloudletIds;
        }

        //????????????????????????cloudlet
        if(Constant.USING_EXISTANCE_CLOULETS){
            if(serialObjectHandler.checkObjectExist(Constant.SERIAL_CLOUDLETID_PATH)){
                Constant.CLOUDLETID_EXIST = true;
                cloudletIds = serialObjectHandler.reverseSerializableObject(Constant.SERIAL_CLOUDLETID_PATH);
            }
        }

        for(String TRACE_FILENAME: googleTraceHandler.getTRACE_FILENAMES()){
            GoogleTaskEventsTraceReader reader = GoogleTaskEventsTraceReader.getInstance(simulation, TRACE_FILENAME,this::createCloudlet);
            if(Constant.USING_EXISTANCE_CLOULETS && Constant.CLOUDLETID_EXIST){
                reader.setPredicate(event -> cloudletIds.contains(event.getUniqueTaskId()) && event.getTimestamp() <= Constant.STOP_TIME);
            }else{
                if(Constant.READ_INITIAL_MACHINE_CLOUDLET){
                    if(Constant.USING_GOOGLE_HOST){
                        reader.setPredicate(event -> (hostIds.contains(event.getMachineId()) && event.getTimestamp() <= Constant.STOP_TIME));
                    }else{
                        reader.setPredicate(event -> (event.getTimestamp() <= Constant.STOP_TIME));
                        reader.setMaxCloudletsToCreate(200);
                    }
                }else{
                    if(Constant.USING_EXISTANCE_PRECLOULETS){
                        reader.setPredicate(event -> ( finalEligibleCloudletIds.contains(event.getUniqueTaskId()) && event.getTimestamp() <= Constant.STOP_TIME));
                    }else{
                        reader.setPredicate(event -> event.getTimestamp() <= Constant.STOP_TIME);
                        reader.setMaxCloudletsToCreate(100);
                    }
//                reader.setMaxCloudletsToCreate(100);
                }
            }
            if(broker != null){
                reader.setDefaultBroker(broker);
            }
            Collection<Cloudlet> eachfileCloudlets = reader.process();
            cloudlets.addAll(eachfileCloudlets);
            if(broker == null){
                List<DatacenterBroker> eachfileBrokers = reader.getBrokers();
                brokers.addAll(eachfileBrokers);
                System.out.printf(
                    "TaskEventFile??? %d Cloudlets and %d Brokers created from the %s trace file.%n",
                    eachfileCloudlets.size(), eachfileBrokers.size(), TRACE_FILENAME);
            }else{
                System.out.printf(
                    "TaskEventFile??? %d Cloudlets and using Default Brokers which created from the %s trace file.%n",
                    eachfileCloudlets.size(), TRACE_FILENAME);
            }
        }
        cloudlets.forEach(cloudlet -> cloudletIds.add(cloudlet.getId()));
        System.out.printf(
            "Total %d Cloudlets and %d Brokers created!%n",
            cloudlets.size(),brokers.size());
    }


    private Cloudlet createCloudlet(final TaskEvent event) {
        final long pesNumber = positive(event.actualCpuCores(Constant.VM_PES), Constant.VM_PES);
        final double maxRamUsagePercent = positive(event.getResourceRequestForRam(), Conversion.HUNDRED_PERCENT);
        final UtilizationModelDynamic utilizationRam = new UtilizationModelDynamic(0, maxRamUsagePercent);
//        final double sizeInMB    = event.getResourceRequestForLocalDiskSpace() * Constant.VM_SIZE_MB[0] + 1;
        final double sizeInMB    = 1;   //????????????CPU???MEM????????????????????????????????????????????????1mb????????????
        final long   sizeInBytes = (long) Math.ceil(megaBytesToBytes(sizeInMB));
        return new CloudletSimple(Constant.CLOUDLET_LENGTH, pesNumber)
            .setFileSize(sizeInBytes)
            .setOutputSize(sizeInBytes)
            .setUtilizationModelBw(UtilizationModel.NULL) //????????????CPU???MEM?????????BW??????????????????null
            .setUtilizationModelCpu(new UtilizationModelDynamic(0.9))
            .setUtilizationModelRam(utilizationRam)
//            .addOnUpdateProcessingListener(dataCenterPrinter::onUpdateCloudletProcessingListener)
            ;
    }

    private void createGoogleDatacenters() {
        datacenters = new ArrayList<>(Constant.DATACENTERS_NUMBER);
        hostIds = new HashSet<>(Constant.HOST_SIZE);
        final GoogleMachineEventsTraceReader reader = GoogleMachineEventsTraceReader.getInstance(Constant.MACHINE_FILENAME , this::createHost);
        reader.setMaxRamCapacity(Constant.MAX_HOST_MEM);
        reader.setMaxCpuCores(Constant.MAX_HOST_CORES);
        reader.setMaxLinesToRead(Constant.GOOGLE_MACHINE_LINES_FILE);

        //Creates Datacenters with no hosts.
        for(int i=0;i<Constant.DATACENTERS_NUMBER;++i){
            this.allocationPolicy =
                new VmAllocationPolicyMigrationBestFitStaticThreshold(
                    new VmSelectionPolicyMinimumUtilization(),
                    //???????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
                    Constant.HOST_CPU_OVER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION + 0.2);
            Log.setLevel(VmAllocationPolicy.LOGGER, Level.WARN);
            this.allocationPolicy.setUnderUtilizationThreshold(Constant.HOST_CPU_UNDER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION);
            Datacenter datacenter = new DatacenterSimple(simulation,allocationPolicy);
            datacenter
                .setSchedulingInterval(Constant.SCHEDULING_INTERVAL)
//                .setHostSearchRetryDelay(Constant.HOST_SEARCH_RETRY_DELAY)
                ;
            datacenters.add(datacenter);
        }

        reader.setDatacenterForLaterHosts(datacenters.get(0));
        List<Host> totalReadHosts = new ArrayList<>(reader.process());
        if(Constant.NUMBER_RANDOM_HOSTS){
            hostList = googleTraceHandler.randomChooseHostsFromGoogleHosts(totalReadHosts,hostIds);
        }else{
            int trueSize = Math.min(totalReadHosts.size(), Constant.HOST_SIZE);
            hostList = new ArrayList<>(trueSize);
            for(int i=0;i<trueSize;++i){
                hostList.add(totalReadHosts.get(i));
            }
        }
        System.out.println();
        System.out.printf("# Created %d Hosts that were immediately available from the Google trace file%n", hostList.size());
//        System.out.printf("# %d Hosts will be available later on (according to the trace timestamp)%n", reader.getNumberOfLaterAvailableHosts());
//        System.out.printf("# %d Hosts will be removed later on (according to the trace timestamp)%n%n", reader.getNumberOfHostsForRemoval());
        //Finally, the immediately created Hosts are added to the first Datacenter
        datacenters.get(0).addHostList(hostList);
        dataCenterPrinter.printHostsInformation(hostList);
    }

    private void createModifyDatacenters() {
        datacenters = new ArrayList<>(Constant.DATACENTERS_NUMBER);
        hostIds = new HashSet<>(Constant.HOSTS);
        hostList = new ArrayList<>(Constant.HOSTS);
        int halfNumOfHost = Constant.HOSTS/2;
        for(int i=0;i<halfNumOfHost;++i){
            Host host1 = createHost(0);
            Host host2 = createHost(1);
            hostList.add(host1);
            hostList.add(host2);
        }
        System.out.println();
        System.out.printf("# Created %d Hosts from modified setting%n", hostList.size());
        for(int i=0;i<Constant.DATACENTERS_NUMBER;++i){
            this.allocationPolicy =
                new VmAllocationPolicyMigrationBestFitStaticThreshold(
                    new VmSelectionPolicyMinimumUtilization(),
                    //???????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
                    Constant.HOST_CPU_OVER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION + 0.2);
            Log.setLevel(VmAllocationPolicy.LOGGER, Level.WARN);
            this.allocationPolicy.setUnderUtilizationThreshold(Constant.HOST_CPU_UNDER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION);
            Datacenter datacenter = new DatacenterSimple(simulation,allocationPolicy);
            datacenter
                .setSchedulingInterval(Constant.SCHEDULING_INTERVAL)
//                .setHostSearchRetryDelay(Constant.HOST_SEARCH_RETRY_DELAY)
            ;
            datacenters.add(datacenter);
        }
        //??????????????????datacenter????????????????????????hostlist
        datacenters.get(0).addHostList(hostList);
        dataCenterPrinter.printHostsInformation(hostList);
    }

    private Host createHost(final MachineEvent event) {
        final PowerModelHost powerModel = new PowerModelHostSimple(Constant.MAX_POWER,Constant.STATIC_POWER);
        final Host host = new HostSimple(event.getRam(), Constant.MAX_HOST_BW, Constant.MAX_HOST_STORAGE, createPesList(event.getCpuCores()));
        host
            .setVmScheduler(new VmSchedulerTimeShared())
            .setRamProvisioner(new ResourceProvisionerSimple())
            .setBwProvisioner(new ResourceProvisionerSimple())
            .setPowerModel(powerModel);
//        //host???????????????????????????
//        final boolean activateHost = true;
//        host.setActive(activateHost);
//
//        //??????????????????????????????????????????
//        final int shutdownDeadlineSeconds = 1;
//        host.setIdleShutdownDeadline(shutdownDeadlineSeconds);
        host.setId(event.getMachineId());
        hostIds.add(host.getId());
        host.enableStateHistory();
        host.enableUtilizationStats();
        return host;
    }
    private Host createHost(int hostType) {
        final PowerModelHost powerModel = new PowerModelHostSimple(Constant.MAX_POWER,Constant.STATIC_POWER);
//        final PowerModelHost powerModelHost = new PowerModelHostSpec();
        final Host host = new HostSimple(Constant.HOST_RAM[hostType], Constant.HOST_BW[hostType], Constant.HOST_STORAGE[hostType], createPesList(Constant.HOST_PES,hostType));
        host
            .setVmScheduler(new VmSchedulerTimeShared())
            .setRamProvisioner(new ResourceProvisionerSimple())
            .setBwProvisioner(new ResourceProvisionerSimple())
            .setPowerModel(powerModel);
//        //host???????????????????????????
//        final boolean activateHost = true;
//        host.setActive(activateHost);
//
//        //??????????????????????????????????????????
//        final int shutdownDeadlineSeconds = 1;
//        host.setIdleShutdownDeadline(shutdownDeadlineSeconds);

        //??????host??????????????????
        host.enableStateHistory();
        host.enableUtilizationStats();
        return host;
    }

    private List<Pe> createPesList(final int count) {
        List<Pe> cpuCoresList = new ArrayList<>(count);
        for(int i = 0; i < count; i++){
            cpuCoresList.add(new PeSimple(Constant.MAX_HOST_MIPS, new PeProvisionerSimple()));
        }
        return cpuCoresList;
    }
    private List<Pe> createPesList(final int count,int hostType) {
        List<Pe> cpuCoresList = new ArrayList<>(count);
        for(int i = 0; i < count; i++){
            cpuCoresList.add(new PeSimple(Constant.HOST_MIPS[hostType], new PeProvisionerSimple()));
        }
        return cpuCoresList;
    }

    private void readTaskUsageTraceFile() throws IOException {
        for(String eachFileUsageName: googleTraceHandler.getUsage_FILENAMES()){
            GoogleTaskUsageTraceReader reader = GoogleTaskUsageTraceReader.getInstance(brokers, eachFileUsageName);
            if(Constant.USING_FILTER){
                if(Constant.FILTER_INSIDE_CLOUDLET){
                    reader.setPredicate(taskUsage ->
                        cloudletIds.contains(taskUsage.getUniqueTaskId()) &&
                            (taskUsage.getMeanCpuUsageRate()>0.05 && taskUsage.getMeanCpuUsageRate()<0.9) &&
                            (taskUsage.getCanonicalMemoryUsage()>0.05 && taskUsage.getCanonicalMemoryUsage()<0.9) &&
                            taskUsage.getStartTime() < Constant.STOP_TIME);
                }else{
                    reader.setPredicate(taskUsage ->{
                        if(!cloudletIds.contains(taskUsage.getUniqueTaskId()) || taskUsage.getStartTime() > Constant.STOP_TIME || taskUsage.getStartTime() == 0.0) return false;
                        if(!Constant.CLOUDLETID_EXIST){
                            if(taskUsage.getMeanCpuUsageRate()<0.05 || taskUsage.getMeanCpuUsageRate()>0.9 ||
                                taskUsage.getCanonicalMemoryUsage()<0.05 || taskUsage.getCanonicalMemoryUsage()>0.9){
                                cloudletIds.remove(taskUsage.getUniqueTaskId());
                                return false;
                            }
                        }
                        return true;
                    });
                }
            }else{
                reader.setPredicate(taskUsage ->
                    cloudletIds.contains(taskUsage.getUniqueTaskId()));
            }
            final Collection<Cloudlet> processedCloudlets = reader.process();
            System.out.printf("TraceFile??? %d Cloudlets processed from the %s trace file.%n", processedCloudlets.size(), eachFileUsageName);
            System.out.printf("TraceFile??? current %d CloudletsIds in the System %n", cloudletIds.size());
        }
        if(Constant.USING_FILTER && !Constant.FILTER_INSIDE_CLOUDLET){
            cloudlets.removeIf(cloudlet -> !cloudletIds.contains(cloudlet.getId()));
            System.out.printf("Total %d Cloudlets and %d Brokers created!%n", cloudlets.size(),brokers.size());
        }
        if(Constant.USING_FILTER && !Constant.CLOUDLETID_EXIST){
            serialObjectHandler.serializableObject(cloudletIds,Constant.SERIAL_CLOUDLETID_PATH);
        }
    }

    private List<Vm> createVms() {
        //?????????????????????vms????????????
        return IntStream.range(0, Constant.VMS).mapToObj(this::createVm).collect(Collectors.toList());
    }

    private Vm createVm(final int id) {
        //Uses a CloudletSchedulerTimeShared by default
        Random r = new Random(System.currentTimeMillis());
        int type = r.nextInt(4);
//        return new VmSimple(Constant.VM_MIPS_M, Constant.VM_PES_M).setRam(Constant.VM_RAM_M).setBw(Constant.VM_BW[0]).setSize(Constant.VM_SIZE_MB[0]);
        Vm vm = new VmSimple(Constant.VM_MIPS[type], Constant.VM_PES).setRam(Constant.VM_RAM[type]).setBw(Constant.VM_BW[type]).setSize(Constant.VM_SIZE_MB[type]);
        vm.enableUtilizationStats();
        return vm;
    }

    public void createAndSubmitVms(DatacenterBroker broker) {
        //???????????????0.2s????????????
        //broker.setVmDestructionDelay(0.2);
        final List<Vm> list = IntStream.range(0, Constant.VMS).mapToObj(this::createVm).collect(Collectors.toList());
        vmList.addAll(list);
        broker.submitVmList(list);
        list.forEach(vm -> vm.addOnMigrationStartListener(this::startMigration));
        list.forEach(vm -> vm.addOnMigrationFinishListener(this::finishMigration));
    }

    /**
     * Event listener which is called every time the simulation clock advances.
     * Then, if the time defined in the Constant.SCHEDULING_INTERVAL has passed,
     * something happen
     *
     * @param info information about the event happened.
     */
    private void clockTickListener(final EventInfo info) {
        final double time = Math.floor(info.getTime());
        if(time > lastClockTime && time % Constant.SCHEDULING_INTERVAL == 0) {
            System.out.println();
        }
        lastClockTime = time;
    }

    /**
     * A listener method that is called when a VM migration starts.
     * @param info information about the happened event
     *
     * @see #createAndSubmitVms(DatacenterBroker)
     * @see Vm#addOnMigrationFinishListener(EventListener)
     */
    private void startMigration(final VmHostEventInfo info) {
        final Vm vm = info.getVm();
        final Host targetHost = info.getHost();
        System.out.printf(
            "# %.2f: %s started migrating to %s (you can perform any operation you want here)%n",
            info.getTime(), vm, targetHost);
        showVmAllocatedMips(vm, targetHost, info.getTime());
        //VM current host (source)
        showHostAllocatedMips(info.getTime(), vm.getHost());
        //Migration host (target)
        showHostAllocatedMips(info.getTime(), targetHost);
        System.out.println();

        migrationsNumber++;
//        if(migrationsNumber > 1){
//            return;
//        }

//        //After the first VM starts being migrated, tracks some metrics along simulation time
//        simulation.addOnClockTickListener(clock -> {
//            if (clock.getTime() <= 2 || (clock.getTime() >= 11 && clock.getTime() <= 15))
//                showVmAllocatedMips(vm, targetHost, clock.getTime());
//        });
    }

    private void showVmAllocatedMips(final Vm vm, final Host targetHost, final double time) {
        final String msg = String.format("# %.2f: %s in %s: total allocated", time, vm, targetHost);
        final MipsShare allocatedMips = targetHost.getVmScheduler().getAllocatedMips(vm);
        final String msg2 = allocatedMips.totalMips() == vm.getMips() * 0.9 ? " - reduction due to migration overhead" : "";
        System.out.printf("%s %.0f MIPs (divided by %d PEs)%s\n", msg, allocatedMips.totalMips(), allocatedMips.pes(), msg2);
    }

    /**
     * A listener method that is called when a VM migration finishes.
     * @param info information about the happened event
     *
     * @see #createAndSubmitVms(DatacenterBroker)
     * @see Vm#addOnMigrationStartListener(EventListener)
     */
    private void finishMigration(final VmHostEventInfo info) {
        final Host host = info.getHost();
        System.out.printf(
            "# %.2f: %s finished migrating to %s (you can perform any operation you want here)%n",
            info.getTime(), info.getVm(), host);
        System.out.print("\t\t");
        showHostAllocatedMips(info.getTime(), hostList.get(1));
        System.out.print("\t\t");
        showHostAllocatedMips(info.getTime(), host);
    }

    private void showHostAllocatedMips(final double time, final Host host) {
        System.out.printf(
            "%.2f: %s allocated %.2f MIPS from %.2f total capacity%n",
            time, host, host.getTotalAllocatedMips(), host.getTotalMipsCapacity());
    }

    /**
     * A listener that is called after all VMs from a broker are created,
     * setting the allocation policy to the default value
     * so that some Hosts will be overloaded with the placed VMs and migration will be fired.
     *
     * The listener is removed after finishing, so that it's called just once,
     * even if new VMs are submitted and created latter on.
     */
    private void onVmsCreatedListener(final DatacenterBrokerEventInfo info) {
        allocationPolicy.setOverUtilizationThreshold(Constant.HOST_CPU_OVER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION);
        broker.removeOnVmsCreatedListener(info.getListener());
        vmList.forEach(vm -> showVmAllocatedMips(vm, vm.getHost(), info.getTime()));

        System.out.println();
        hostList.forEach(host -> showHostAllocatedMips(info.getTime(), host));
        System.out.println();
    }

}
