package org.cloudsimplus.MyExample;

import ch.qos.logback.classic.Level;
import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicy;
import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigrationDynamicUpperThresholdFirstFit;
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
import org.cloudbus.cloudsim.power.models.PowerModelHostSpec;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.ResourceProvisionerSimple;
import org.cloudbus.cloudsim.resources.*;
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerSpaceShared;
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerTimeSharedOverSubscription;
import org.cloudbus.cloudsim.selectionpolicies.*;
import org.cloudbus.cloudsim.util.Conversion;
import org.cloudbus.cloudsim.util.TimeUtil;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModel;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelDynamic;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelFull;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelStochastic;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmSimple;
import org.cloudsimplus.MyExample.modifyMigration.*;
import org.cloudsimplus.listeners.*;
import org.cloudsimplus.listeners.EventListener;
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

public class myImplementationMigrationDatacenter {

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
    public static MathHandler mathHandler;      //??????????????????
    private double lastClockTime;   //?????????????????????
    private VmAllocationPolicyMigrationStaticThreshold allocationPolicy;    //????????????
    private VmAllocationPolicyMigrationDynamicUpperThresholdFirstFit dynamicUpperAllocationPolicy;
    private VmAllocationPolicy noMigrationAllocationPolicy;
    private int migrationsNumber = 0;   //????????????
    private double totalEnergyCumsumption = 0.0;
    private static Set<Double> existTimes = new HashSet<>();
    List<Cloudlet> totalCloudlets;
    List<Long> activeHostNumber = new ArrayList<>();
    List<Double> resourceWastageList = new ArrayList<>();
    private double preClockTime = -1.0;
    private double preLogClockTime = -1.0;
    private boolean dvfs = false;
    private boolean npa = false;
    private boolean myVersion = false;
    private boolean dynamicUpperThreshold = false;
    public List<Double> hostusages = new ArrayList<>();
    public List<Double> hostusagesram = new ArrayList<>();
    public List<Double> hostusages2 = new ArrayList<>();
    public List<Double> hostusagesram2 = new ArrayList<>();
    public List<Double> hostusages3 = new ArrayList<>();
    public List<Double> hostusagesram3 = new ArrayList<>();
    public List<Double> hostusages4 = new ArrayList<>();
    public List<Double> hostusagesram4 = new ArrayList<>();
    public List<Double> hostusages5 = new ArrayList<>();
    public List<Double> hostusagesram5 = new ArrayList<>();

    /**
     * A map to store RAM utilization history for every VM.
     * Each key is a VM and each value is another map.
     * This entire data structure is usually called a multi-map.
     *
     * Such an internal map stores RAM utilization for a VM.
     * The keys of this internal map are the time the utilization was collected (in seconds)
     * and the value the utilization percentage (from 0 to 1).
     */
//    private static Map<Vm, Map<Double, Double>> allVmsRamUtilizationHistory;
//    private static Map<Host,Map<Double,Double>> allHostsRamUtilizationHistory;
    private static Map<Host,LinkedList<Double>> allHostsRamUtilizationHistoryQueue;
    private static Map<Host,LinkedList<Double>> allHostsCpuUtilizationHistoryQueue;
    private static Map<Vm,LinkedList<Double>> allVmsRamUtilizationHistoryQueue;
    private static Map<Vm,LinkedList<Double>> allVmsCpuUtilizationHistoryQueue;
//    private static Map<Host,ArrayList<Double>> allHostsRamUtilizationHistoryAL;
//    private static Map<Host,ArrayList<Double>> allHostsCpuUtilizationHistoryAL;

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
        mathHandler = new MathHandler();

        //????????????????????????
        if(Constant.TEST_TRACE){
            googleTraceHandler.buildTraceFileNamesSample();
        }else{
            googleTraceHandler.buildTraceFileNames();
        }

        //????????????????????????
        new myImplementationMigrationDatacenter();
    }

    private myImplementationMigrationDatacenter() throws IOException, ClassNotFoundException {

        //???????????????????????????????????????
        final double startSecs = TimeUtil.currentTimeSecs();
        System.out.printf("Simulation started at %s%n%n", LocalTime.now());
        Log.setLevel(DatacenterBroker.LOGGER, Level.TRACE);
        Log.setLevel(Level.TRACE);

        //??????????????????
        simulation = new CloudSim();

        //?????????????????????
        createUtilizationHistory();

        //????????????????????????????????????????????????????????????????????????
        if(Constant.USING_GOOGLE_HOST){
            createGoogleDatacenters();
        }else{
            createModifyDatacenters();
            hostList.forEach(host-> hostIds.add(host.getId()));
        }

        //???Google????????????????????????????????????cloudlet??????
        createCloudletsAndBrokersFromTraceFileType1();

        //??????vm???????????????cloudlet
        //???????????????????????????????????????
        brokers.forEach(this::createAndSubmitVms);
//        brokers.forEach(broker->broker.setFailedVmsRetryDelay(-1));

        //?????????cpu,ram??????
        initializeUtilizationHistory();

        //????????????????????????
        simulation.addOnClockTickListener(this::clockTickListener);

        //???GoogleUsageTrace???????????????Cloudlet????????????
        readTaskUsageTraceFile();

        //??????brokers???cloudlets?????????
        System.out.println("Brokers:");
        brokers.stream().sorted().forEach(b -> System.out.printf("\t%d - %s%n", b.getId(), b.getName()));
        System.out.println("Cloudlets:");
        cloudlets.stream().sorted().forEach(c -> System.out.printf("\t%s (job %d)%n", c, c.getJobId()));

        //???????????????????????????
        brokers.forEach(broker -> broker.addOnVmsCreatedListener(this::onVmsCreatedListener));
//
        //????????????????????????????????????
        //?????????????????????????????????
//        PowerMeter powerMeter = new PowerMeter(simulation, datacenters);
//        powerMeter.setMeasurementInterval(Constant.COLLECTTIME);

        //????????????????????????????????????
        simulation.terminateAt(Constant.STOP_TIME);

        //???????????????????????????
        simulation.start();
//        //????????????cloudlet????????????
//        brokers.stream().sorted().forEach(broker->dataCenterPrinter.printCloudlets(broker));

//        //???????????????????????????????????????
//        hostList.stream().forEach(this::printHostInfo);
//
//        //????????????
//        dataCenterPrinter.printVmsCpuUtilizationAndPowerConsumption(brokers);
//        dataCenterPrinter.printHostsCpuUtilizationAndPowerConsumption(hostList);

        //?????????????????????????????????
//        totalEnergyCumsumption = dataCenterPrinter.printDataCenterTotalEnergyComsumption(powerMeter,npa);
        totalEnergyCumsumption = dataCenterPrinter.dataCenterTotalEnergyComsumption(datacenters.get(0),npa);

        //??????????????????
        System.out.printf("Number of VM migrations: %d%n", migrationsNumber);

        //??????????????????
        final double endSecs = TimeUtil.currentTimeSecs();
        System.out.printf("Simulation finished at %s. Execution time: %.2f seconds%n", LocalTime.now(), TimeUtil.elapsedSeconds(startSecs));

//        for(Host host:hostList){
//            System.out.println("host:" +host.getId()+" over 100 ???????????????" + host.getTotalOver100Time());
//        }

//        hostList.forEach(host -> System.out.println(host.getTotalUpTime()));

        dataCenterPrinter.printNewSLAV(hostList,vmList,totalEnergyCumsumption,migrationsNumber);
        dataCenterPrinter.calculateAverageActiveHost(activeHostNumber,hostList.size());
        dataCenterPrinter.printSystemAverageResourceWastage(resourceWastageList);
        dataCenterPrinter.printTtoalShutdownHostNumber(hostList);

        writeUsage(1, hostusages, hostusagesram);
        writeUsage(2, hostusages2, hostusagesram2);
        writeUsage(3, hostusages3, hostusagesram3);
        writeUsage(4, hostusages4, hostusagesram4);
        writeUsage(5, hostusages5, hostusagesram5);
        //???????????????????????????????????????
//        dataCenterPrinter.activeHostCount(hostList);

//        //??????host???cpu?????????
//        System.setOut(new PrintStream(new FileOutputStream(Constant.HOST_LOG_FILE_PATH)));
//        System.out.printf("%nHosts CPU usage History (when the allocated MIPS is lower than the requested, it is due to VM migration overhead)%n");
//        hostList.forEach(host->dataCenterPrinter.printHostStateHistory(host,allHostsRamUtilizationHistory.get(host)));
    }

    public void writeUsage(int index, List<Double> hostusages, List<Double> hostusagesram) throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter("D:\\java_workspace\\cloudsimplus-6.3.0-MyExample\\cloudsim-plus-examples\\src\\main\\java\\org\\cloudsimplus\\MyExample\\logs\\originalcpu" + index + ".csv"));

        for (Double line : hostusages) {
            bw.write(String.valueOf(line));
            bw.newLine();
            bw.flush();
        }

        bw = new BufferedWriter(new FileWriter("D:\\java_workspace\\cloudsimplus-6.3.0-MyExample\\cloudsim-plus-examples\\src\\main\\java\\org\\cloudsimplus\\MyExample\\logs\\originalram" + index + ".csv"));
        for (Double line : hostusagesram) {
            bw.write(String.valueOf(line));
            bw.newLine();
            bw.flush();
        }

        //????????????
        bw.close();
    }

    private void createCloudletsAndBrokersFromTraceFileType1() throws IOException, ClassNotFoundException {
        cloudlets = new HashSet<>();
        cloudletIds = new HashSet<>();
        brokers = new ArrayList<>();

        //??????????????????
        if(Constant.SINGLE_BROKER){
            broker = new DatacenterBrokerSimple(simulation);
            brokers.add(broker);
        }

        if(!Constant.USING_TEST_CLOUDLET){
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
                        }
                reader.setMaxCloudletsToCreate(1569);
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
        }else{
            totalCloudlets = new ArrayList<>();
            for(int i=0;i<1000;++i){
                Cloudlet cloudlet = createCloudlet();
                totalCloudlets.add(cloudlet);
                cloudlets.add(cloudlet);
            }
            totalCloudlets.forEach(cloudlet -> cloudletIds.add(cloudlet.getId()));
            broker.submitCloudletList(totalCloudlets);
        }
    }


    private Cloudlet createCloudlet(final TaskEvent event) {
        final long pesNumber = positive(event.actualCpuCores(Constant.VM_PES), Constant.VM_PES);
        double RamUsagePercent = event.getResourceRequestForRam();
        double CpuUsagePercent = event.getResourceRequestForCpuCores();
        RamUsagePercent = Math.max(Math.min(RamUsagePercent,1), 0.05);
        CpuUsagePercent = Math.max(Math.min(CpuUsagePercent,1), 0.05);
//        System.out.println(CpuUsagePercent+" "+RamUsagePercent);
        final UtilizationModelDynamic utilizationCpu = new UtilizationModelDynamic(CpuUsagePercent,Conversion.HUNDRED_PERCENT);
        final UtilizationModelDynamic utilizationRam = new UtilizationModelDynamic(RamUsagePercent,Conversion.HUNDRED_PERCENT);
//        final double sizeInMB    = event.getResourceRequestForLocalDiskSpace() * Constant.VM_SIZE_MB[0] + 1;
        final double sizeInMB    = 1;   //????????????CPU???MEM????????????????????????????????????????????????1mb????????????
        final long   sizeInBytes = (long) Math.ceil(megaBytesToBytes(sizeInMB));
        Cloudlet cloudlet = new CloudletSimple(Constant.CLOUDLET_LENGTH, pesNumber)
            .setFileSize(sizeInBytes)
            .setOutputSize(sizeInBytes)
            .setUtilizationModelBw(UtilizationModel.NULL) //????????????CPU???MEM?????????BW??????????????????null
            .setUtilizationModelCpu(utilizationCpu)
            .setUtilizationModelRam(utilizationRam);
        //            .addOnUpdateProcessingListener(dataCenterPrinter::onUpdateCloudletProcessingListener);
        cloudlet.addOnFinishListener(info -> {
            Vm vm = info.getVm();
            System.out.printf(
                "%n# %.2f: Intentionally destroying %s due to cloudlet finished.",
                info.getTime(), vm);
            System.out.println();
            if(!vm.isInMigration()){
                vm.getHost().destroyVm(vm);
            }
            vm.setDestory(true);
        });
        return cloudlet;
    }
    private Cloudlet createCloudlet() {
        final long length = Constant.TEST_CLOUDLET_LENGTH;
        final long fileSize = 300;
        final long outputSize = 300;
//        UtilizationModel utilizationModelDynamic = new UtilizationModelDynamic(0.5);
//        UtilizationModel utilizationModelCpu = new UtilizationModelDynamic(0.5);
        UtilizationModelStochastic utilizationCpu = new UtilizationModelStochastic();
//        utilizationCpu.setOverCapacityRequestAllowed(true);
        UtilizationModelStochastic utilizationRam = new UtilizationModelStochastic();
        utilizationRam.setHistoryEnabled(false);
        utilizationCpu.setHistoryEnabled(false);

        Cloudlet cloudlet = new CloudletSimple(length, 1);
        cloudlet.setFileSize(fileSize)
            .setOutputSize(outputSize)
            .setUtilizationModelCpu(new UtilizationModelDynamic(0.1))
            .setUtilizationModelBw(new UtilizationModelFull())
            .setUtilizationModelRam(new UtilizationModelDynamic(0.1));
        cloudlet.addOnFinishListener(info -> {
            Vm vm = info.getVm();
            System.out.printf(
                "%n# %.2f: Intentionally destroying %s on %s due to cloudlet finished.",
                info.getTime(), vm,vm.getHost());
            if(!vm.isInMigration()){
                vm.getHost().destroyVm(vm);
            }
            vm.setDestory(true);
        });
        return cloudlet;
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
                new VmAllocationPolicyMigrationStaticThreshold(
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
        hostIds = new HashSet<>();
        hostList = new ArrayList<>();
        int halfNumOfHost = Constant.HOSTS/2;
        for(int i=0;i<halfNumOfHost;++i){
            Host host1 = createHost(0);
            Host host2 = createHost(1);
//            host1.setIdlePower(Constant.IDLE_POWER[0]);
//            host2.setIdlePower(Constant.IDLE_POWER[1]);
            hostList.add(host1);
            hostList.add(host2);
        }
        System.out.println();
        System.out.printf("# Created %d Hosts from modified setting%n", hostList.size());
        for(int i=0;i<Constant.DATACENTERS_NUMBER;++i){

//            //NPA??????
//            npa = true;
//            this.noMigrationAllocationPolicy = new VmAllocationPolicyNPA();

//            //DVFS??????
//            dvfs = true;
//            this.noMigrationAllocationPolicy = new VmAllocationPolicyDVFS();

//            //FFD???????????? + static??????
//            this.allocationPolicy =
//                new VmAllocationPolicyMigrationFirstFitStaticThreshold(
//                    new VmSelectionPolicyUnbalanceUtilization(),
//                    //???????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????jintia
//                    Constant.HOST_CPU_OVER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION + 0.1);

            //PABFD + static?????? ????????????
            this.allocationPolicy =
                new VmAllocationPolicyPowerAwereMigrationBestFitStaticThreshold(
                    new VmSelectionPolicyMC(new VmSelectionPolicyMinimumMigrationTime()),
                    //???????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
                    Constant.HOST_CPU_OVER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION + 0.3,
                    mathHandler,
                    allHostsRamUtilizationHistoryQueue,
                    allHostsCpuUtilizationHistoryQueue,
                    allVmsRamUtilizationHistoryQueue,
                    allVmsCpuUtilizationHistoryQueue);
            this.allocationPolicy.setRamOverUtilizationThreshold(Constant.HOST_RAM_OVER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION+0.3);
            this.allocationPolicy.setUnderUtilizationThreshold(Constant.HOST_CPU_UNDER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION,Constant.HOST_RAM_UNDER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION);

//            //PABFD + ??????T?????????
//            myVersion = true;
//            this.allocationPolicy =
//                new VmAllocationPolicyPowerAwereMigrationBestFitHalfDynamicThreshold(
//                    new VmSelectionPolicyMC(new VmSelectionPolicyMinimumMigrationTime()),
//                    //???????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
//                    Constant.HOST_CPU_OVER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION + 0.2,
//                    mathHandler);
//            this.allocationPolicy.setRamOverUtilizationThreshold(Constant.HOST_RAM_OVER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION+0.2);
//            this.allocationPolicy.setUnderUtilizationThreshold(Constant.HOST_CPU_UNDER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION,Constant.HOST_RAM_UNDER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION);

            //?????????????????????static?????????dynamic??????
            //PABFD + MAD??????
            dynamicUpperThreshold = true;
            this.dynamicUpperAllocationPolicy =
                new VmAllocationPolicyPowerAwereMigrationBestFitMADThreshold(
                    new VmSelectionPolicyMC(new VmSelectionPolicyMinimumMigrationTime()),
                    2.5,
                    allocationPolicy,
                    mathHandler,
                    allHostsRamUtilizationHistoryQueue,
                    allHostsCpuUtilizationHistoryQueue,
                    allVmsRamUtilizationHistoryQueue,
                    allVmsCpuUtilizationHistoryQueue);


//            //PABFD + IQR??????
//            dynamicUpperThreshold = true;
//            this.dynamicUpperAllocationPolicy =
//                new VmAllocationPolicyPowerAwereMigrationBestFitIQRThreshold(
//                    new VmSelectionPolicyMC(new VmSelectionPolicyMinimumMigrationTime()),
//                    1.5,
//                    allocationPolicy,
//                    mathHandler,
//                    allHostsRamUtilizationHistoryQueue,
//                    allHostsCpuUtilizationHistoryQueue,
//                    allVmsRamUtilizationHistoryQueue,
//                    allVmsCpuUtilizationHistoryQueue);

//            //PABFD + LR??????
//            dynamicUpperThreshold = true;
//            this.dynamicUpperAllocationPolicy =
//                new VmAllocationPolicyPowerAwereMigrationBestFitLRThreshold(
//                    new VmSelectionPolicyMC(new VmSelectionPolicyMinimumMigrationTime()),
//                    1.2,
//                    allocationPolicy,
//                    mathHandler,
//                    allHostsRamUtilizationHistoryQueue,
//                    allHostsCpuUtilizationHistoryQueue,
//                    allVmsRamUtilizationHistoryQueue,
//                    allVmsCpuUtilizationHistoryQueue);

//            //PABFD + WMA??????
//            dynamicUpperThreshold = true;
//            this.dynamicUpperAllocationPolicy =
//                new VmAllocationPolicyPowerAwereMigrationBestFitWMAThreshold(
//                    new VmSelectionPolicyMinimumMigrationTime(),
//                    1.0,
//                    allocationPolicy,
//                    mathHandler,
//                    allHostsRamUtilizationHistoryQueue,
//                    allHostsCpuUtilizationHistoryQueue,
//                    allVmsRamUtilizationHistoryQueue,
//                    allVmsCpuUtilizationHistoryQueue);

            Log.setLevel(VmAllocationPolicy.LOGGER, Level.WARN);

            //?????????????????????
            if(!Constant.USING_UNDERLOAD_THRESHOLD){
                this.allocationPolicy.setEnableMigrateMostUnbalanceWastageLoadHost(true);
                if(dynamicUpperAllocationPolicy != null)
                    this.dynamicUpperAllocationPolicy.setEnableMigrateMostUnbalanceWastageLoadHost(true);
            }

            //???ram????????????
            if(Constant.USING_RAM){
                this.allocationPolicy.setHostRamThreshold(true);
                if(dynamicUpperAllocationPolicy != null)
                    this.dynamicUpperAllocationPolicy.setHostRamThreshold(true);
            }

            Datacenter datacenter;
            if(!dvfs && !npa){
                if(dynamicUpperThreshold){
                    datacenter = new DatacenterSimple(simulation,hostList,dynamicUpperAllocationPolicy);
                }else{
                    datacenter = new DatacenterSimple(simulation,hostList,allocationPolicy);
                }
            }else{
                datacenter = new DatacenterSimple(simulation,hostList,noMigrationAllocationPolicy);
            }

            datacenter
                .setSchedulingInterval(Constant.SCHEDULING_INTERVAL)
//                .setHostSearchRetryDelay(Constant.HOST_SEARCH_RETRY_DELAY)
            ;
            datacenters.add(datacenter);
        }
//        //??????????????????datacenter????????????????????????hostlist
        //?????????????????????datacenter?????????host???????????????simulation
//        datacenters.get(0).addHostList(hostList);
        dataCenterPrinter.printHostsInformation(hostList);
    }


    private Host createHost(final MachineEvent event) {
        final PowerModelHost powerModel = new PowerModelHostSimple(Constant.MAX_POWER,Constant.STATIC_POWER);
        final Host host = new HostSimple(event.getRam(), Constant.MAX_HOST_BW, Constant.MAX_HOST_STORAGE, createPesList(event.getCpuCores()));
        host
            .setVmScheduler(new VmSchedulerSpaceShared())
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
//        host.enableStateHistory();
//        host.enableUtilizationStats();
        return host;
    }
    private Host createHost(int hostType) {
        PowerModelHost powerModel;
        if(hostType == 0){
//            powerModel = new PowerModelHostSimple(Constant.HOST_G4_SPEC_POWER[Constant.HOST_G4_SPEC_POWER.length-1],Constant.IDLE_POWER[0]);
            powerModel = new PowerModelHostSpec(Arrays.asList(Constant.HOST_G4_SPEC_POWER));
        }else{
//            powerModel = new PowerModelHostSimple(Constant.HOST_G5_SPEC_POWER[Constant.HOST_G5_SPEC_POWER.length-1],Constant.IDLE_POWER[0]);
            powerModel = new PowerModelHostSpec(Arrays.asList(Constant.HOST_G5_SPEC_POWER));
        }
        final Host host = new HostSimple(Constant.HOST_RAM[hostType], Constant.HOST_BW[hostType], Constant.HOST_STORAGE[hostType], createPesList(Constant.HOST_PES,hostType));
        host
            .setRamProvisioner(new ResourceProvisionerSimple())
            .setBwProvisioner(new ResourceProvisionerSimple())
            .setVmScheduler(new VmSchedulerTimeSharedOverSubscription())
            .setPowerModel(powerModel);
        host.setIdleShutdownDeadline(Constant.IDLE_SHUTDOWN_TIME);
//        host.addOnUpdateProcessingListener(this::updateHostResource);
//        host.setLazySuitabilityEvaluation(true);
        //host???????????????????????????
//        final boolean activateHost = true;
//        host.setActive(activateHost);
//
//        //??????????????????????????????????????????
//        final int shutdownDeadlineSeconds = 1;
//        host.setIdleShutdownDeadline(shutdownDeadlineSeconds);

        //??????host??????????????????
        host.enableStateHistory();

        //??????cpu??????????????????????????????
//        host.enableUtilizationStats();
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
//        List<Vm> vmList = new ArrayList<>();
//        int id = 0;
//        for(int i=0;i<Constant.VM_NUMBER.length;++i){
//            for(int j=0;j<Constant.VM_NUMBER[i];++j){
//                vmList.add(createVm(id));
//            }
//        }
//        return vmList;
        //?????????????????????vms????????????
        return IntStream.range(0, Constant.VMS).mapToObj(this::createVm).collect(Collectors.toList());
    }

    private Vm createVm(final int id) {
        int type = id / (int) Math.ceil((double) Constant.VMS / Constant.VM_TYPE.length);
        Vm vm = new VmSimple(Constant.VM_MIPS[type], Constant.VM_PES);
        vm
            .setRam(Constant.VM_RAM[type]).setBw(Constant.VM_BW[type])
            .setSize(0);
        vm.setMinCpuUtilization(10/(double)Constant.VM_MIPS[type]);
        vm.setMinRamUtilization(10/(double)Constant.VM_RAM[type]);
        vm.setLogLength(Constant.VM_LogLength);
        return vm;
    }

    private Vm createVm(final int id,final int type) {
        //Uses a CloudletSchedulerTimeShared by default
//        Random r = new Random(System.currentTimeMillis());
//        int type = r.nextInt(4);
//        return new VmSimple(Constant.VM_MIPS_M, Constant.VM_PES_M).setRam(Constant.VM_RAM_M).setBw(Constant.VM_BW[0]).setSize(Constant.VM_SIZE_MB[0]);
        Vm vm = new VmSimple(Constant.VM_MIPS[type], Constant.VM_PES);
        vm
            .setRam(Constant.VM_RAM[type]).setBw(Constant.VM_BW[type])
            .setSize(0);
        vm.setMinCpuUtilization(10/(double)Constant.VM_MIPS[type]);
        vm.setMinRamUtilization(10/(double)Constant.VM_RAM[type]);
//        vm.enableUtilizationStats();
        return vm;
    }

    public void createAndSubmitVms(DatacenterBroker broker) {
        //???????????????0.2s????????????
//        broker.setVmDestructionDelay(0.2);


        vmList.addAll(createVms());
//        vmList.sort((k,v)-> (int)(v.getRam().getCapacity()-k.getRam().getCapacity()));
        broker.submitVmList(vmList);

        vmList.forEach(vm -> vm.addOnMigrationStartListener(this::startMigration));
        vmList.forEach(vm -> vm.addOnMigrationFinishListener(this::finishMigration));
    }

    /**
     * Event listener which is called every time the simulation clock advances.
     * Then, if the time defined in the Constant.SCHEDULING_INTERVAL has passed,
     * something happen
     *
     * @param info information about the event happened.
     */
    private void clockTickListener(final EventInfo info) {
        double time = simulation.clock();
        int currentTime = (int)time;
        if(time - currentTime != 0.0 && currentTime == preClockTime) return;
        long number = dataCenterPrinter.activeHostCount(hostList,simulation.clockStr());
        activeHostNumber.add(number);
        if(currentTime % Constant.SCHEDULING_INTERVAL == 0){

//            System.out.println(currentTime+" ???????????????");
            //?????????????????????????????????????????????
            calculateResourceWastage(hostList,resourceWastageList);

//            //??????host???vm????????????
//            collectHostResourceUtilization();
            double systemWastage = 0.0;

//            for(Host host:hostList){
//
//                LinkedList<Double> hostRamhistory = allHostsRamUtilizationHistoryQueue.get(host);
//                LinkedList<Double> hostCpuhistory = allHostsCpuUtilizationHistoryQueue.get(host);
//
//                if(host.isActive()){
//
//                    //??????host????????????
//                    systemWastage += host.resourceWastage();
//
//                    //??????SLATH
//                    double hostRamUtilization = host.getRamPercentUtilization();
//                    double hostCpuUtilization = host.getCpuPercentUtilization();
//
////                    if(hostRamUtilization >= 0.85 || hostCpuUtilization >= 0.85){
////                        host.setTotalOver100Time(host.getTotalOver100Time() + Constant.SCHEDULING_INTERVAL);
////                    }
//
//                    //??????host???vm?????????
//                    hostRamhistory.addLast(hostRamUtilization);
//                    hostCpuhistory.addLast(hostCpuUtilization);
//                    host.getVmList().forEach(vm -> {
//                        if(vm.getHost().getId() == host.getId()){
//                            LinkedList<Double> vmRamHistory = allVmsRamUtilizationHistoryQueue.get(vm);
//                            LinkedList<Double> vmCpuHistory = allVmsCpuUtilizationHistoryQueue.get(vm);
//                            double vmCpuUtilization = vm.getCpuPercentUtilization();
//                            double vmRamUtilization = vm.getCurrentRequestedRam()/(double)vm.getRam().getCapacity();
//                            vmCpuHistory.addLast(vmCpuUtilization);
//                            vmRamHistory.addLast(vmRamUtilization);
//                            while(vmCpuHistory.size() > Constant.VM_LogLength){
//                                vmCpuHistory.removeFirst();
//                            }
//                            while(vmRamHistory.size() > Constant.VM_LogLength){
//                                vmRamHistory.removeFirst();
//                            }
//                        }
//                    });
//                    while(hostRamhistory.size() > Constant.HOST_LogLength){
//                        hostRamhistory.removeFirst();
//                    }
//                    while(hostCpuhistory.size() > Constant.HOST_LogLength){
//                        hostCpuhistory.removeFirst();
//                    }
//                }else{
//                    hostRamhistory.clear();
//                    hostCpuhistory.clear();
//                }
//            }

            resourceWastageList.add(systemWastage);
            putUsage(439, hostusages, hostusagesram);
            putUsage(0, hostusages2, hostusagesram2);
            putUsage(195, hostusages3, hostusagesram3);
            putUsage(211, hostusages4, hostusagesram4);
            putUsage(243, hostusages5, hostusagesram5);
        }
        preClockTime = currentTime;
    }

    public void putUsage(int index, List<Double> hostusages, List<Double> hostusagesram)
    {
        double usage =  hostList.get(index).getCpuPercentUtilization();
        double usageram = hostList.get(index).getRamPercentUtilization();
        hostusages.add(usage);
        hostusagesram.add(usageram);
//        System.out.println(usage + "   " + usageram);
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
        dataCenterPrinter.showVmAllocatedMips(vm, targetHost, info.getTime());
        //VM current host (source)
        dataCenterPrinter.showHostAllocatedMips(info.getTime(), vm.getHost());
        //Migration host (target)
        dataCenterPrinter.showHostAllocatedMips(info.getTime(), targetHost);
        System.out.println();

//        if(simulation.clock() > 599.0){
            migrationsNumber++;
//        }
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
        final Vm vm = info.getVm();
        System.out.printf(
            "# %.2f: %s finished migrating to %s (you can perform any operation you want here)%n",
            info.getTime(), info.getVm(), host);
        System.out.print("\t\t");
        dataCenterPrinter.showHostAllocatedMips(info.getTime(), host);
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
        if(!dvfs && !npa){
            allocationPolicy.setOverUtilizationThreshold(Constant.HOST_CPU_OVER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION);
            allocationPolicy.setRamOverUtilizationThreshold(Constant.HOST_RAM_OVER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION);
        }
        //?????????????????????????????????????????????????????????????????????host???cpu???ram????????????0.8
        if(myVersion){
            hostList.stream().forEach(host -> {
                host.setCPU_THRESHOLD(Constant.HOST_CPU_OVER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION );
                host.setRAM_THRESHOLD(Constant.HOST_RAM_OVER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION );
            });
        }
        broker.removeOnVmsCreatedListener(info.getListener());
        vmList.forEach(vm -> dataCenterPrinter.showVmAllocatedMips(vm, vm.getHost(), info.getTime()));
        System.out.println();
        hostList.forEach(host -> dataCenterPrinter.showHostAllocatedMips(info.getTime(), host));
        System.out.println();
    }

    public void initializeUtilizationHistory() {
        System.out.println(simulation.clockStr()+": ?????????????????????vm???host???????????????");
        hostList.forEach(host -> allHostsRamUtilizationHistoryQueue.put(host,new LinkedList<>()));
        hostList.forEach(host -> allHostsCpuUtilizationHistoryQueue.put(host,new LinkedList<>()));
        vmList.forEach(vm->allVmsCpuUtilizationHistoryQueue.put(vm,new LinkedList<>()));
        vmList.forEach(vm->allVmsRamUtilizationHistoryQueue.put(vm,new LinkedList<>()));
    }
    public void createUtilizationHistory(){
        allHostsRamUtilizationHistoryQueue = new HashMap<>(Constant.HOSTS);
        allHostsCpuUtilizationHistoryQueue = new HashMap<>(Constant.HOSTS);
        allVmsRamUtilizationHistoryQueue = new HashMap<>(Constant.VMS);
        allVmsCpuUtilizationHistoryQueue = new HashMap<>(Constant.VMS);
    }

    public void calculateResourceWastage(List<Host> hostList,List<Double> resourceWastageList){
        double systemWastage = 0.0;
        for(Host host:hostList){
            if(host.isActive()){
                systemWastage += host.resourceWastage();
            }
        }
        resourceWastageList.add(systemWastage);
    }


}
