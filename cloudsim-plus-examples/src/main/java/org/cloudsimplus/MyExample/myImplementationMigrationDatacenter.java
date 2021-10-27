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
import org.cloudbus.cloudsim.power.models.PowerModelHostSpec;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.ResourceProvisionerSimple;
import org.cloudbus.cloudsim.resources.*;
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerSpaceShared;
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerTimeSharedOverSubscription;
import org.cloudbus.cloudsim.selectionpolicies.VmSelectionPolicyMinimumUtilization;
import org.cloudbus.cloudsim.util.Conversion;
import org.cloudbus.cloudsim.util.TimeUtil;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModel;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelDynamic;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelFull;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelStochastic;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmSimple;
import org.cloudsimplus.MyExample.modifyMigration.VmAllocationPolicyMigrationFirstFitStaticThreshold;
import org.cloudsimplus.MyExample.modifyMigration.VmAllocationPolicyPASUP;
import org.cloudsimplus.MyExample.modifyMigration.VmAllocationPolicyPowerAwereMigrationBestFitStaticThreshold;
import org.cloudsimplus.MyExample.modifyMigration.VmSelectionPolicyUnbalanceUtilization;
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
    private int migrationsNumber = 0;   //迁移次数
    private double totalEnergyCumsumption = 0.0;
    private static Set<Double> existTimes = new HashSet<>();
    List<Cloudlet> totalCloudlets;
    List<Long> activeHostNumber = new ArrayList<>();

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
//        Log.setLevel(DatacenterBroker.LOGGER, Level.TRACE);
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
        PowerMeter powerMeter = new PowerMeter(simulation, datacenters);
        powerMeter.setMeasurementInterval(Constant.SCHEDULING_INTERVAL);

        //系统在第一天结束停止运行
//        simulation.terminateAt(Constant.STOP_TIME);

        //数据中心模拟器启动
        simulation.start();

//        //打印所有cloudlet运行状况
        brokers.stream().sorted().forEach(broker->dataCenterPrinter.printCloudlets(broker));

//        //打印生成的服务器的配置信息
//        hostList.stream().forEach(this::printHostInfo);
//
//        //打印能耗
//        dataCenterPrinter.printVmsCpuUtilizationAndPowerConsumption(brokers);
//        dataCenterPrinter.printHostsCpuUtilizationAndPowerConsumption(hostList);

        //计算并打印数据中心能耗
        totalEnergyCumsumption = dataCenterPrinter.printDataCenterTotalEnergyComsumption(powerMeter);

        //打印迁移次数
        System.out.printf("Number of VM migrations: %d%n", migrationsNumber);

        //记录结束时间
        final double endSecs = TimeUtil.currentTimeSecs();
        System.out.printf("Simulation finished at %s. Execution time: %.2f seconds%n", LocalTime.now(), TimeUtil.elapsedSeconds(startSecs));

//        for(Host host:hostList){
//            System.out.println("host:" +host.getId()+" over 100 的总时间：" + host.getTotalOver100Time());
//        }

//        hostList.forEach(host -> System.out.println(host.getTotalUpTime()));

        dataCenterPrinter.calculateSLAV(hostList,vmList,totalEnergyCumsumption,migrationsNumber);

        //打印当前系统活跃的主机数目
//        dataCenterPrinter.activeHostCount(hostList);

//        //打印host的cpu利用率
//        System.setOut(new PrintStream(new FileOutputStream(Constant.HOST_LOG_FILE_PATH)));
//        System.out.printf("%nHosts CPU usage History (when the allocated MIPS is lower than the requested, it is due to VM migration overhead)%n");
//        hostList.forEach(host->dataCenterPrinter.printHostStateHistory(host,allHostsRamUtilizationHistory.get(host)));
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
        RamUsagePercent = (RamUsagePercent > 1 ? Conversion.HUNDRED_PERCENT : RamUsagePercent);
        final UtilizationModelDynamic utilizationRam = new UtilizationModelDynamic(RamUsagePercent,Conversion.HUNDRED_PERCENT);
//        final double sizeInMB    = event.getResourceRequestForLocalDiskSpace() * Constant.VM_SIZE_MB[0] + 1;
        final double sizeInMB    = 1;   //如只研究CPU和MEM，磁盘空间不考虑的话，象征性给个1mb意思一下
        final long   sizeInBytes = (long) Math.ceil(megaBytesToBytes(sizeInMB));
        return new CloudletSimple(Constant.CLOUDLET_LENGTH, pesNumber)
            .setFileSize(sizeInBytes)
            .setOutputSize(sizeInBytes)
            .setUtilizationModelBw(UtilizationModel.NULL) //如只研究CPU和MEM，忽略BW，所以设置为null
            .setUtilizationModelCpu(new UtilizationModelDynamic(1))
            .setUtilizationModelRam(new UtilizationModelDynamic(1))
//            .addOnUpdateProcessingListener(dataCenterPrinter::onUpdateCloudletProcessingListener)
            ;
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
//        utilizationRam.setOverCapacityRequestAllowed(true);

        Cloudlet cloudlet = new CloudletSimple(length, 1);
        cloudlet.setFileSize(fileSize)
            .setOutputSize(outputSize)
            .setUtilizationModelCpu(utilizationCpu)
            .setUtilizationModelBw(UtilizationModel.NULL)
            .setUtilizationModelRam(utilizationRam);
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
            host1.setIdlePower(Constant.IDLE_POWER[0]);
            host2.setIdlePower(Constant.IDLE_POWER[1]);
            hostList.add(host1);
            hostList.add(host2);
        }
        System.out.println();
        System.out.printf("# Created %d Hosts from modified setting%n", hostList.size());
        for(int i=0;i<Constant.DATACENTERS_NUMBER;++i){

            this.allocationPolicy =
                new VmAllocationPolicyMigrationBestFitStaticThreshold(
                    new VmSelectionPolicyMinimumUtilization(),
                    //策略刚开始阈值会比设定值大一点，以放置虚拟机。当所有虚拟机提交到主机后，阈值就会变回设定值
                    Constant.HOST_CPU_OVER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION + 0.1);

//            this.allocationPolicy =
//                new VmAllocationPolicyMigrationFirstFitStaticThreshold(
//                    new VmSelectionPolicyMinimumUtilization(),
//                    //策略刚开始阈值会比设定值大一点，以放置虚拟机。当所有虚拟机提交到主机后，阈值就会变回设定值
//                    Constant.HOST_CPU_OVER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION + 0.1);

//            this.allocationPolicy =
//                new VmAllocationPolicyPowerAwereMigrationBestFitStaticThreshold(
//                    new VmSelectionPolicyMinimumUtilization(),
//                    //策略刚开始阈值会比设定值大一点，以放置虚拟机。当所有虚拟机提交到主机后，阈值就会变回设定值
//                    Constant.HOST_CPU_OVER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION + 0.1);

//            this.allocationPolicy =
//                new VmAllocationPolicyPASUP(
//                    new VmSelectionPolicyUnbalanceUtilization(),
//                    //策略刚开始阈值会比设定值大一点，以放置虚拟机。当所有虚拟机提交到主机后，阈值就会变回设定值
//                    Constant.HOST_CPU_OVER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION + 0.1,
//                    mathHandler,
//                    allHostsRamUtilizationHistoryQueue,
//                    allHostsCpuUtilizationHistoryQueue,
//                    allVmsRamUtilizationHistoryQueue,
//                    allVmsCpuUtilizationHistoryQueue);

            Log.setLevel(VmAllocationPolicy.LOGGER, Level.WARN);

            //把ram判断阈值
            this.allocationPolicy.setHostRamThreshold(true);

            //低阈值迁移只迁移一个
//            this.allocationPolicy.setEnableMigrateOneUnderLoadHost(true);

            this.allocationPolicy.setUnderUtilizationThreshold(Constant.HOST_CPU_UNDER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION,Constant.HOST_RAM_UNDER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION);
            Datacenter datacenter = new DatacenterSimple(simulation,hostList,allocationPolicy);
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
            powerModel = new PowerModelHostSpec(Arrays.asList(Constant.HOST_G4_SPEC_POWER));
        }else{
            powerModel = new PowerModelHostSpec(Arrays.asList(Constant.HOST_G5_SPEC_POWER));
        }
        final Host host = new HostSimple(Constant.HOST_RAM[hostType], Constant.HOST_BW[hostType], Constant.HOST_STORAGE[hostType], createPesList(Constant.HOST_PES,hostType));
        host
            .setRamProvisioner(new ResourceProvisionerSimple())
            .setBwProvisioner(new ResourceProvisionerSimple())
            .setVmScheduler(new VmSchedulerTimeShared())
            .setPowerModel(powerModel);
        host.setIdleShutdownDeadline(Constant.IDLE_SHUTDOWN_TIME);
        host.addOnUpdateProcessingListener(this::updateHostResource);
//        host.setLazySuitabilityEvaluation(true);
        //host创建之后的活跃状态
//        final boolean activateHost = true;
//        host.setActive(activateHost);
//
//        //当虚拟机限制多久之后会被关闭
//        final int shutdownDeadlineSeconds = 1;
//        host.setIdleShutdownDeadline(shutdownDeadlineSeconds);

        //启用host记录历史状态
//        host.enableStateHistory();
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
        //每个代理都创建vms个虚拟机
        return IntStream.range(0, Constant.VMS).mapToObj(this::createVm).collect(Collectors.toList());
    }

    private Vm createVm(final int id) {
        //Uses a CloudletSchedulerTimeShared by default
//        Random r = new Random(System.currentTimeMillis());
//        int type = r.nextInt(4);
        int type = id % Constant.VM_TYPE.length;
//        return new VmSimple(Constant.VM_MIPS_M, Constant.VM_PES_M).setRam(Constant.VM_RAM_M).setBw(Constant.VM_BW[0]).setSize(Constant.VM_SIZE_MB[0]);
        Vm vm = new VmSimple(Constant.VM_MIPS[type], Constant.VM_PES);
        vm
            .setRam(Constant.VM_RAM[type]).setBw(Constant.VM_BW[type])
            .setSize(Constant.VM_SIZE_MB[type]);
//        vm.enableUtilizationStats();
        return vm;
    }

    public void createAndSubmitVms(DatacenterBroker broker) {
        //虚拟机闲置0.2s之后销毁
//        broker.setVmDestructionDelay(0.2);


        vmList.addAll(createVms());
        vmList.sort((k,v)-> (int)(v.getRam().getCapacity()-k.getRam().getCapacity()));
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
//        double systemTime = simulation.clock();
//        //收集每个时刻每个虚拟机的RAM利用率
//        collectVmResourceUtilization(allVmsRamUtilizationHistory, systemTime, Ram.class);
//        //收集每个时刻每个host的RAM利用率
//        collectHostRamResourceUtilization(systemTime);

//        collectHostRamResourceUtilization();
//        collectHostCpuResourceUtilization();
        double time = simulation.clock();
//        hostList.forEach(host -> {
//            double hostRamUtilization = host.getRamPercentUtilization();
//            double hostCpuUtilization = host.getCpuPercentUtilization();
//            if(hostCpuUtilization >= 1.0 || hostRamUtilization >= 1.0) host.setTotalOver100Time(host.getTotalOver100Time()+Constant.SCHEDULING_INTERVAL);
//        });
        if(time - (int)time != 0.0) return;
//        vmList.forEach(vm->{
//            System.out.println(simulation.clockStr()+" "+vm+" 容量："+vm.getRam().getCapacity());
//            System.out.println(simulation.clockStr()+" "+vm+" 请求的："+ vm.getCurrentRequestedRam());
//        });
//        if(existTimes.contains(time)) return;
//        else existTimes.add(time);
        if((int)time % Constant.HOST_Log_INTERVAL == 0){
//            collectHostResourceUtilization();
            hostList.forEach(host->{
                if(vmList.isEmpty() && host.isIdleEnough(host.getIdleShutdownDeadline()) && !host.hasMigratingVms()){
                    host.setActive(false);
                }
//                System.out.println(host+" "+host.getRam().getAvailableResource());
            });
            long number = dataCenterPrinter.activeHostCount(hostList,simulation.clockStr());
            activeHostNumber.add(number);
//            dataCenterPrinter.activeVmsCount(hostList,simulation.clockStr());
//            System.out.println();
//            System.out.println("前");
//            for(double num:allHostsRamUtilizationHistoryQueue.get(hostList.get(1))){
//                System.out.println(num);
//            }
//            System.out.println("后");
//            System.out.println();
        }
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

        migrationsNumber++;
    }

    private void updateHostResource(final HostUpdatesVmsProcessingEventInfo info) {
        final Host host = info.getHost();

        LinkedList<Double> hostRamhistory = allHostsRamUtilizationHistoryQueue.get(host);
        LinkedList<Double> hostCpuhistory = allHostsCpuUtilizationHistoryQueue.get(host);


        if(host.isActive()){
//                System.out.println("打印开机时间："+host.getTotalUpTime());
//            if(hostRamhistory.size() >= Constant.HOST_LogLength * 2){
            while(hostRamhistory.size() > Constant.HOST_LogLength-1){
                hostRamhistory.removeFirst();
                hostCpuhistory.removeFirst();
            }
//            }
            double hostRamUtilization = host.getRamPercentUtilization();
            double hostCpuUtilization = host.getCpuPercentUtilization();

            if(hostRamUtilization >= 1.0 || hostCpuUtilization >= 1.0){
                host.setTotalOver100Time(host.getTotalOver100Time() + Constant.SCHEDULING_INTERVAL);
            }

//                System.out.println(simulation.clockStr() + ": host" + host.getId() + " "+hostCpuUtilization + "   "+hostRamUtilization);
//                if(hostCpuUtilization == 1.0 || hostRamUtilization == 1.0) host.setTotalOver100Time(host.getTotalOver100Time()+Constant.SCHEDULING_INTERVAL);
            hostRamhistory.addLast(hostRamUtilization);
            hostCpuhistory.addLast(hostCpuUtilization);
//                System.out.println("当前时间是："+simulation.clockStr()+"  host id: " +host.getId());
            host.getVmList().forEach(vm -> {
                LinkedList<Double> vmRamHistory = allVmsRamUtilizationHistoryQueue.get(vm);
                LinkedList<Double> vmCpuHistory = allVmsCpuUtilizationHistoryQueue.get(vm);
//                if(vmCpuHistory.size() >= Constant.VM_LogLength * 2){
                while(vmCpuHistory.size() > Constant.VM_LogLength-1){
                    vmCpuHistory.removeFirst();
                    vmRamHistory.removeFirst();
                }
//                }


                //更新vm总共请求的mips数目
                vm.setTotalrequestUtilization(vm.getTotalrequestUtilization() + vm.getTotalCpuMipsUtilization()* Constant.SCHEDULING_INTERVAL);

                vmCpuHistory.addLast(vm.getCpuPercentUtilization());
                vmRamHistory.addLast(vm.getRam().getPercentUtilization());
            });
        }else{
            while(hostRamhistory.size() > Constant.HOST_LogLength-1){
                hostRamhistory.removeFirst();
                hostCpuhistory.removeFirst();
            }
            hostRamhistory.addLast(0.0);
            hostCpuhistory.addLast(0.0);
        }

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
        dataCenterPrinter.showHostAllocatedMips(info.getTime(), hostList.get(1));
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
        allocationPolicy.setOverUtilizationThreshold(Constant.HOST_CPU_OVER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION);
        allocationPolicy.setRamOverUtilizationThreshold(Constant.HOST_RAM_OVER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION);
        broker.removeOnVmsCreatedListener(info.getListener());
        vmList.forEach(vm -> dataCenterPrinter.showVmAllocatedMips(vm, vm.getHost(), info.getTime()));
        System.out.println();
        hostList.forEach(host -> dataCenterPrinter.showHostAllocatedMips(info.getTime(), host));
        System.out.println();
    }

    public void initializeUtilizationHistory() {
//        allVmsRamUtilizationHistory = new HashMap<>(Constant.VMS);
//        vmList.forEach(vm -> allVmsRamUtilizationHistory.put(vm, new TreeMap<>()));
//        allHostsRamUtilizationHistory = new HashMap<>(800);
//        hostList.forEach(host -> allHostsRamUtilizationHistory.put(host,new TreeMap<>()));
        System.out.println(simulation.clockStr()+": 当前正在初始化vm和host利用率记录");
        hostList.forEach(host -> allHostsRamUtilizationHistoryQueue.put(host,new LinkedList<>()));
        hostList.forEach(host -> allHostsCpuUtilizationHistoryQueue.put(host,new LinkedList<>()));
        vmList.forEach(vm->allVmsCpuUtilizationHistoryQueue.put(vm,new LinkedList<>()));
        vmList.forEach(vm->allVmsRamUtilizationHistoryQueue.put(vm,new LinkedList<>()));
//        allHostsRamUtilizationHistoryAL = new HashMap<>(Constant.HOSTS);
//        allHostsCpuUtilizationHistoryAL = new HashMap<>(Constant.HOSTS);
//        hostList.forEach(host -> allHostsRamUtilizationHistoryAL.put(host,new ArrayList<>()));
//        hostList.forEach(host -> allHostsCpuUtilizationHistoryAL.put(host,new ArrayList<>()));
    }
    public void createUtilizationHistory(){
        allHostsRamUtilizationHistoryQueue = new HashMap<>(Constant.HOSTS);
        allHostsCpuUtilizationHistoryQueue = new HashMap<>(Constant.HOSTS);
        allVmsRamUtilizationHistoryQueue = new HashMap<>(Constant.VMS);
        allVmsCpuUtilizationHistoryQueue = new HashMap<>(Constant.VMS);
    }

//    private void collectVmResourceUtilization(final Map<Vm, Map<Double, Double>> allVmsUtilizationHistory, double systemTime ,Class<? extends ResourceManageable> resourceClass) {
//        vmList.forEach(vm -> allVmsUtilizationHistory.get(vm).put(systemTime, vm.getResource(resourceClass).getPercentUtilization()));
//    }
//
//    private void collectHostRamResourceUtilization(double systemTime){
//        hostList.forEach(host -> allHostsRamUtilizationHistory.get(host).put(systemTime,host.getRamPercentUtilization()));
//    }

    private void collectHostRamResourceUtilization(){
        hostList.forEach(host -> {
            LinkedList<Double> hostRamhistory = allHostsRamUtilizationHistoryQueue.get(host);
            if(hostRamhistory.size()<Constant.HOST_LogLength){
                hostRamhistory.addLast(host.getRamPercentUtilization());
            }else{
                hostRamhistory.removeFirst();
                hostRamhistory.addLast(host.getRamPercentUtilization());
            }
        });
    }

    private void collectHostResourceUtilization(){
        hostList.forEach(host -> {
            LinkedList<Double> hostRamhistory = allHostsRamUtilizationHistoryQueue.get(host);
            LinkedList<Double> hostCpuhistory = allHostsCpuUtilizationHistoryQueue.get(host);


            if(host.isActive()){
//                System.out.println("打印开机时间："+host.getTotalUpTime());
//            if(hostRamhistory.size() >= Constant.HOST_LogLength * 2){
                while(hostRamhistory.size() > Constant.HOST_LogLength-1){
                    hostRamhistory.removeFirst();
                    hostCpuhistory.removeFirst();
                }
//            }
                double hostRamUtilization = host.getRamPercentUtilization();
                double hostCpuUtilization = host.getCpuPercentUtilization();
//                System.out.println(simulation.clockStr() + ": host" + host.getId() + " "+hostCpuUtilization + "   "+hostRamUtilization);
//                if(hostCpuUtilization == 1.0 || hostRamUtilization == 1.0) host.setTotalOver100Time(host.getTotalOver100Time()+Constant.SCHEDULING_INTERVAL);
                hostRamhistory.addLast(hostRamUtilization);
                hostCpuhistory.addLast(hostCpuUtilization);
//                System.out.println("当前时间是："+simulation.clockStr()+"  host id: " +host.getId());
                host.getVmList().forEach(vm -> {
                    LinkedList<Double> vmRamHistory = allVmsRamUtilizationHistoryQueue.get(vm);
                    LinkedList<Double> vmCpuHistory = allVmsCpuUtilizationHistoryQueue.get(vm);
//                if(vmCpuHistory.size() >= Constant.VM_LogLength * 2){
                    while(vmCpuHistory.size() > Constant.VM_LogLength-1){
                        vmCpuHistory.removeFirst();
                        vmRamHistory.removeFirst();
                    }
//                }


                    //更新vm总共请求的mips数目
                    vm.setTotalrequestUtilization(vm.getTotalrequestUtilization() + vm.getTotalCpuMipsUtilization()* Constant.SCHEDULING_INTERVAL);

                    vmCpuHistory.addLast(vm.getCpuPercentUtilization());
                    vmRamHistory.addLast(vm.getRam().getPercentUtilization());
                });
            }else{
                while(hostRamhistory.size() > Constant.HOST_LogLength-1){
                    hostRamhistory.removeFirst();
                    hostCpuhistory.removeFirst();
                }
                hostRamhistory.addLast(0.0);
                hostCpuhistory.addLast(0.0);
            }
        });
    }

//    private void collectHostResourceUtilization(){
//        hostList.forEach(host -> {
//            ArrayList<Double> hostRamhistory = allHostsRamUtilizationHistoryAL.get(host);
//            ArrayList<Double> hostCpuhistory = allHostsCpuUtilizationHistoryAL.get(host);
//            if(hostRamhistory.size()<Constant.LogLength){
//                hostRamhistory.add(host.getRamPercentUtilization());
//                hostCpuhistory.add(host.getCpuPercentUtilization());
//            }else{
//                hostRamhistory.remove(0);
//                hostRamhistory.add(host.getRamPercentUtilization());
//                hostCpuhistory.remove(0);
//                hostCpuhistory.add(host.getCpuPercentUtilization());
//            }
//        });
//    }

    private void collectHostCpuResourceUtilization(){
        hostList.forEach(host -> {
            LinkedList<Double> hostCpuhistory = allHostsCpuUtilizationHistoryQueue.get(host);
            double utilization = host.getCpuPercentUtilization();
            if (hostCpuhistory.size() >= Constant.HOST_LogLength) {
                hostCpuhistory.removeFirst();
            }
            hostCpuhistory.addLast(utilization);
        });
    }

//    //直接移除不对齐的cpu和ram利用率，不建议开启
//    private void processHostUsage(){
//        for(Host host:hostList){
//            List<HostStateHistoryEntry> hostStateHistoryEntries = host.getStateHistory();
//            Map<Double,Double> hostRamUtilizationHistory = allHostsRamUtilizationHistory.get(host);
//            Iterator<HostStateHistoryEntry> itHistory = hostStateHistoryEntries.iterator();
//            while(itHistory.hasNext()){
//                var history = itHistory.next();
//                double time = history.getTime();
//                double ramUsage = hostRamUtilizationHistory.get(time);
//                double cpuUsage = history.getAllocatedMips()/host.getTotalMipsCapacity()*100;
//                if(ramUsage == 0.0 && cpuUsage != 0.0){
//                    itHistory.remove();
//                    hostRamUtilizationHistory.remove(time);
//                    continue;
//                }
//                if(ramUsage != 0.0 && cpuUsage == 0.0){
//                    itHistory.remove();
//                    hostRamUtilizationHistory.remove(time);
//                }
//            }
//        }
//    }


}
