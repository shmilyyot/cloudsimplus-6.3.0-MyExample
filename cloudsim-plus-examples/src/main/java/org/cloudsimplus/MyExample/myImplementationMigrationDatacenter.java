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
 * 论文实验标准数据中心模板设置
 * 本模板所有参数和功能分支皆可在同一目录下的{@link Constant}文件中修改，无需特意改动本文件
 * 若多个cloudlet分配到同一个vm，cpu资源会对半分，然后继续占各自的部分比例
 * TODO： 还没实现SLA违反指标
 * */

public class myImplementationMigrationDatacenter {

    /**
     * cloudsim仿真数据中心相关设置
     * */
    private final CloudSim simulation;  //仿真启动对象
    private List<Datacenter> datacenters;   //多个数据中心
    private Collection<Cloudlet> cloudlets;    //数据中心的任务
    private List<Host> hostList;    //主机列表
    private List<DatacenterBroker> brokers;    //数据中心的多个代理
    private final List<Vm> vmList = new ArrayList<>();    //虚拟机列表
    private DatacenterBroker broker;    //数据中心的单个代理
    private Set<Long> hostIds;  //数据中心host的id
    private Set<Long> cloudletIds;  //系统中cloudlet的id
    public static GoogleTraceHandler googleTraceHandler;  //处理谷歌数据的代理
    public static serialObject serialObjectHandler;   //处理序列化的代理
    public static DataCenterPrinter dataCenterPrinter;      //处理数据中心打印信息
    public static MathHandler mathHandler;      //处理科学计算
    private double lastClockTime;   //上一个时钟时间
    private VmAllocationPolicyMigrationStaticThreshold allocationPolicy;    //迁移策略
    private VmAllocationPolicyMigrationDynamicUpperThresholdFirstFit dynamicUpperAllocationPolicy;
    private VmAllocationPolicy noMigrationAllocationPolicy;
    private int migrationsNumber = 0;   //迁移次数
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

        //重定向控制台输出到本地log
        if(Constant.PRINT_LOCAL_LOG){
            try{
                System.setOut(new PrintStream(new FileOutputStream(Constant.LOG_FILE_PATH)));
            }catch (FileNotFoundException e){
                e.printStackTrace();
            }
        }

        //创建序列化的代理
        serialObjectHandler = new serialObject();
        //创建谷歌数据处理代理
        googleTraceHandler = new GoogleTraceHandler();
        //创建数据中心打印代理
        dataCenterPrinter = new DataCenterPrinter();
        //创建科学计算代理
        mathHandler = new MathHandler();

        //创建所有文件路径
        if(Constant.TEST_TRACE){
            googleTraceHandler.buildTraceFileNamesSample();
        }else{
            googleTraceHandler.buildTraceFileNames();
        }

        //启动标准数据中心
        new myImplementationMigrationDatacenter();
    }

    private myImplementationMigrationDatacenter() throws IOException, ClassNotFoundException {

        //模拟日志打印，记录开始时间
        final double startSecs = TimeUtil.currentTimeSecs();
        System.out.printf("Simulation started at %s%n%n", LocalTime.now());
        Log.setLevel(DatacenterBroker.LOGGER, Level.TRACE);
        Log.setLevel(Level.TRACE);

        //创建模拟仿真
        simulation = new CloudSim();

        //创建利用率对象
        createUtilizationHistory();

        //使用谷歌数据中心主机模板或自定义数据中心主机模板
        if(Constant.USING_GOOGLE_HOST){
            createGoogleDatacenters();
        }else{
            createModifyDatacenters();
            hostList.forEach(host-> hostIds.add(host.getId()));
        }

        //从Google任务流创建数据中心代理和cloudlet任务
        createCloudletsAndBrokersFromTraceFileType1();

        //创建vm并提交所有cloudlet
        //当虚拟机创建失败，放弃创建
        brokers.forEach(this::createAndSubmitVms);
//        brokers.forEach(broker->broker.setFailedVmsRetryDelay(-1));

        //初始化cpu,ram记录
        initializeUtilizationHistory();

        //添加定时监听事件
        simulation.addOnClockTickListener(this::clockTickListener);

        //从GoogleUsageTrace读取系统中Cloudlet的利用率
        readTaskUsageTraceFile();

        //打印brokers和cloudlets的信息
        System.out.println("Brokers:");
        brokers.stream().sorted().forEach(b -> System.out.printf("\t%d - %s%n", b.getId(), b.getName()));
        System.out.println("Cloudlets:");
        cloudlets.stream().sorted().forEach(c -> System.out.printf("\t%s (job %d)%n", c, c.getJobId()));

        //虚拟机创建监听事件
        brokers.forEach(broker -> broker.addOnVmsCreatedListener(this::onVmsCreatedListener));
//
        //创建数据中心能耗跟踪模型
        //记录每个数据中心的能耗
//        PowerMeter powerMeter = new PowerMeter(simulation, datacenters);
//        powerMeter.setMeasurementInterval(Constant.COLLECTTIME);

        //系统在第一天结束停止运行
        simulation.terminateAt(Constant.STOP_TIME);

        //数据中心模拟器启动
        simulation.start();
//        //打印所有cloudlet运行状况
//        brokers.stream().sorted().forEach(broker->dataCenterPrinter.printCloudlets(broker));

//        //打印生成的服务器的配置信息
//        hostList.stream().forEach(this::printHostInfo);
//
//        //打印能耗
//        dataCenterPrinter.printVmsCpuUtilizationAndPowerConsumption(brokers);
//        dataCenterPrinter.printHostsCpuUtilizationAndPowerConsumption(hostList);

        //计算并打印数据中心能耗
//        totalEnergyCumsumption = dataCenterPrinter.printDataCenterTotalEnergyComsumption(powerMeter,npa);
        totalEnergyCumsumption = dataCenterPrinter.dataCenterTotalEnergyComsumption(datacenters.get(0),npa);

        //打印迁移次数
        System.out.printf("Number of VM migrations: %d%n", migrationsNumber);

        //记录结束时间
        final double endSecs = TimeUtil.currentTimeSecs();
        System.out.printf("Simulation finished at %s. Execution time: %.2f seconds%n", LocalTime.now(), TimeUtil.elapsedSeconds(startSecs));

//        for(Host host:hostList){
//            System.out.println("host:" +host.getId()+" over 100 的总时间：" + host.getTotalOver100Time());
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
        //打印当前系统活跃的主机数目
//        dataCenterPrinter.activeHostCount(hostList);

//        //打印host的cpu利用率
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

        //释放资源
        bw.close();
    }

    private void createCloudletsAndBrokersFromTraceFileType1() throws IOException, ClassNotFoundException {
        cloudlets = new HashSet<>();
        cloudletIds = new HashSet<>();
        brokers = new ArrayList<>();

        //使用单一代理
        if(Constant.SINGLE_BROKER){
            broker = new DatacenterBrokerSimple(simulation);
            brokers.add(broker);
        }

        if(!Constant.USING_TEST_CLOUDLET){
            //使用预处理cloudlet id
            Set<Long> finalEligibleCloudletIds;
            if(Constant.USING_EXISTANCE_PRECLOULETS){
                //这种方法默认读取前n天所有event，会超出java heap空间，所以要曲线救国，先读一遍usage曲线救国
                Set<Long> eligibleCloudletIds;
                //预处理数据，序列化到本地，把usage中所有符合利用率要求的cloudlet id先记录下来，然后再用这些id反向生成cloudlet
                if(serialObjectHandler.checkObjectExist(Constant.SERIAL_PRECLOUDLETID_PATH)){
                    eligibleCloudletIds = serialObjectHandler.reverseSerializableObject(Constant.SERIAL_PRECLOUDLETID_PATH);
                }else{
                    eligibleCloudletIds = googleTraceHandler.filterCloudletsUsageIs(simulation);
                    serialObjectHandler.serializableObject(eligibleCloudletIds,Constant.SERIAL_PRECLOUDLETID_PATH);
                }
                finalEligibleCloudletIds = eligibleCloudletIds;
            }

            //使用已存在的外部cloudlet
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
                        "TaskEventFile： %d Cloudlets and %d Brokers created from the %s trace file.%n",
                        eachfileCloudlets.size(), eachfileBrokers.size(), TRACE_FILENAME);
                }else{
                    System.out.printf(
                        "TaskEventFile： %d Cloudlets and using Default Brokers which created from the %s trace file.%n",
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
        final double sizeInMB    = 1;   //如只研究CPU和MEM，磁盘空间不考虑的话，象征性给个1mb意思一下
        final long   sizeInBytes = (long) Math.ceil(megaBytesToBytes(sizeInMB));
        Cloudlet cloudlet = new CloudletSimple(Constant.CLOUDLET_LENGTH, pesNumber)
            .setFileSize(sizeInBytes)
            .setOutputSize(sizeInBytes)
            .setUtilizationModelBw(UtilizationModel.NULL) //如只研究CPU和MEM，忽略BW，所以设置为null
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
                    //策略刚开始阈值会比设定值大一点，以放置虚拟机。当所有虚拟机提交到主机后，阈值就会变回设定值
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

//            //NPA算法
//            npa = true;
//            this.noMigrationAllocationPolicy = new VmAllocationPolicyNPA();

//            //DVFS算法
//            dvfs = true;
//            this.noMigrationAllocationPolicy = new VmAllocationPolicyDVFS();

//            //FFD放置算法 + static算法
//            this.allocationPolicy =
//                new VmAllocationPolicyMigrationFirstFitStaticThreshold(
//                    new VmSelectionPolicyUnbalanceUtilization(),
//                    //策略刚开始阈值会比设定值大一点，以放置虚拟机。当所有虚拟机提交到主机后，阈值就会变回设定值jintia
//                    Constant.HOST_CPU_OVER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION + 0.1);

            //PABFD + static算法 默认算法
            this.allocationPolicy =
                new VmAllocationPolicyPowerAwereMigrationBestFitStaticThreshold(
                    new VmSelectionPolicyMC(new VmSelectionPolicyMinimumMigrationTime()),
                    //策略刚开始阈值会比设定值大一点，以放置虚拟机。当所有虚拟机提交到主机后，阈值就会变回设定值
                    Constant.HOST_CPU_OVER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION + 0.3,
                    mathHandler,
                    allHostsRamUtilizationHistoryQueue,
                    allHostsCpuUtilizationHistoryQueue,
                    allVmsRamUtilizationHistoryQueue,
                    allVmsCpuUtilizationHistoryQueue);
            this.allocationPolicy.setRamOverUtilizationThreshold(Constant.HOST_RAM_OVER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION+0.3);
            this.allocationPolicy.setUnderUtilizationThreshold(Constant.HOST_CPU_UNDER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION,Constant.HOST_RAM_UNDER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION);

//            //PABFD + 回调T的版本
//            myVersion = true;
//            this.allocationPolicy =
//                new VmAllocationPolicyPowerAwereMigrationBestFitHalfDynamicThreshold(
//                    new VmSelectionPolicyMC(new VmSelectionPolicyMinimumMigrationTime()),
//                    //策略刚开始阈值会比设定值大一点，以放置虚拟机。当所有虚拟机提交到主机后，阈值就会变回设定值
//                    Constant.HOST_CPU_OVER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION + 0.2,
//                    mathHandler);
//            this.allocationPolicy.setRamOverUtilizationThreshold(Constant.HOST_RAM_OVER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION+0.2);
//            this.allocationPolicy.setUnderUtilizationThreshold(Constant.HOST_CPU_UNDER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION,Constant.HOST_RAM_UNDER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION);

            //必须保证有一个static和一个dynamic开着
            //PABFD + MAD算法
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


//            //PABFD + IQR算法
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

//            //PABFD + LR算法
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

//            //PABFD + WMA算法
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

            //使用低负载阈值
            if(!Constant.USING_UNDERLOAD_THRESHOLD){
                this.allocationPolicy.setEnableMigrateMostUnbalanceWastageLoadHost(true);
                if(dynamicUpperAllocationPolicy != null)
                    this.dynamicUpperAllocationPolicy.setEnableMigrateMostUnbalanceWastageLoadHost(true);
            }

            //把ram判断阈值
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
//        //默认只有一个datacenter，所以只提交一个hostlist
        //这样子传没有给datacenter里面的host传入正式的simulation
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
//        //host创建之后的活跃状态
//        final boolean activateHost = true;
//        host.setActive(activateHost);
//
//        //当虚拟机限制多久之后会被关闭
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
        //host创建之后的活跃状态
//        final boolean activateHost = true;
//        host.setActive(activateHost);
//
//        //当虚拟机限制多久之后会被关闭
//        final int shutdownDeadlineSeconds = 1;
//        host.setIdleShutdownDeadline(shutdownDeadlineSeconds);

        //启用host记录历史状态
        host.enableStateHistory();

        //记录cpu历史利用率的一些数据
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
            System.out.printf("TraceFile： %d Cloudlets processed from the %s trace file.%n", processedCloudlets.size(), eachFileUsageName);
            System.out.printf("TraceFile： current %d CloudletsIds in the System %n", cloudletIds.size());
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
        //每个代理都创建vms个虚拟机
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
        //虚拟机闲置0.2s之后销毁
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

//            System.out.println(currentTime+" 发生了收集");
            //每一定时间间隔计算一次资源浪费
            calculateResourceWastage(hostList,resourceWastageList);

//            //统计host和vm的利用率
//            collectHostResourceUtilization();
            double systemWastage = 0.0;

//            for(Host host:hostList){
//
//                LinkedList<Double> hostRamhistory = allHostsRamUtilizationHistoryQueue.get(host);
//                LinkedList<Double> hostCpuhistory = allHostsCpuUtilizationHistoryQueue.get(host);
//
//                if(host.isActive()){
//
//                    //计算host资源浪费
//                    systemWastage += host.resourceWastage();
//
//                    //获取SLATH
//                    double hostRamUtilization = host.getRamPercentUtilization();
//                    double hostCpuUtilization = host.getCpuPercentUtilization();
//
////                    if(hostRamUtilization >= 0.85 || hostCpuUtilization >= 0.85){
////                        host.setTotalOver100Time(host.getTotalOver100Time() + Constant.SCHEDULING_INTERVAL);
////                    }
//
//                    //记录host和vm利用率
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
        //如果使用自己的版本，所有虚拟机创建完之后，每个host的cpu和ram阈值改回0.8
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
        System.out.println(simulation.clockStr()+": 当前正在初始化vm和host利用率记录");
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
