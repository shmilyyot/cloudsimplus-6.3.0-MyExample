package org.cloudsimplus.MyExample;

import ch.qos.logback.classic.Level;
import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicy;
import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicyBestFit;
import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicySimple;
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
import org.cloudbus.cloudsim.power.models.PowerModel;
import org.cloudbus.cloudsim.power.models.PowerModelHost;
import org.cloudbus.cloudsim.power.models.PowerModelHostSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.ResourceProvisionerSimple;
import org.cloudbus.cloudsim.resources.Pe;
import org.cloudbus.cloudsim.resources.PeSimple;
import org.cloudbus.cloudsim.schedulers.MipsShare;
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerSpaceShared;
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.selectionpolicies.VmSelectionPolicyMinimumUtilization;
import org.cloudbus.cloudsim.util.Conversion;
import org.cloudbus.cloudsim.util.TimeUtil;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModel;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelDynamic;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelFull;
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
 * 论文实验标准数据中心模板设置
 * 本模板所有参数和功能分支皆可在同一目录下的{@link Constant}文件中修改，无需特意改动本文件
 * 若多个cloudlet分配到同一个vm，cpu资源会对半分，然后继续占各自的部分比例
 * */

public class standardMigrationDatacenter {

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
    private double lastClockTime;   //上一个时钟时间
    private VmAllocationPolicyMigrationStaticThreshold allocationPolicy;    //迁移策略
    private int migrationsNumber = 0;   //迁移次数

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
        //创建所有文件路径
        googleTraceHandler.buildTraceFileNamesSample();

        //启动标准数据中心
        new standardMigrationDatacenter();
    }

    private standardMigrationDatacenter() throws IOException, ClassNotFoundException {

        //模拟日志打印，记录开始时间
        final double startSecs = TimeUtil.currentTimeSecs();
        System.out.printf("Simulation started at %s%n%n", LocalTime.now());
        Log.setLevel(DatacenterBroker.LOGGER,Level.TRACE);

        //创建模拟仿真
        simulation = new CloudSim();

        //使用谷歌数据中心主机模板或自定义数据中心主机模板
        if(Constant.USING_GOOGLE_HOST){
            createGoogleDatacenters();
        }else{
            createModifyDatacenters();
            hostList.forEach(host-> hostIds.add(host.getId()));
        }

        //从Google任务流创建数据中心代理和cloudlet任务
        createCloudletsAndBrokersFromTraceFileType1();

        //从GoogleUsageTrace读取系统中Cloudlet的利用率
        readTaskUsageTraceFile();

        //创建vm并提交所有cloudlet
        brokers.forEach(this::createAndSubmitVms);

        //打印brokers和cloudlets的信息
        System.out.println("Brokers:");
        brokers.stream().sorted().forEach(b -> System.out.printf("\t%d - %s%n", b.getId(), b.getName()));
        System.out.println("Cloudlets:");
        cloudlets.stream().sorted().forEach(c -> System.out.printf("\t%s (job %d)%n", c, c.getJobId()));

//        //添加定时监听事件
//        simulation.addOnClockTickListener(this::clockTickListener);
        //虚拟机创建监听事件
        brokers.forEach(broker -> broker.addOnVmsCreatedListener(this::onVmsCreatedListener));

        //创建数据中心能耗跟踪模型
        //记录每个数据中心的能耗
        PowerMeter powerMeter = new PowerMeter(simulation, datacenters);

        //数据中心模拟器启动
        simulation.start();

        //打印所有cloudlet运行状况
        brokers.stream().sorted().forEach(broker->dataCenterPrinter.printCloudlets(broker));

//        //打印生成的服务器的配置信息
//        hostList.stream().forEach(this::printHostInfo);

        //打印host的cpu利用率
        System.out.printf("%nHosts CPU usage History (when the allocated MIPS is lower than the requested, it is due to VM migration overhead)%n");
        hostList.forEach(host->dataCenterPrinter.printHostStateHistory(host));

        //打印能耗
        dataCenterPrinter.printVmsCpuUtilizationAndPowerConsumption(brokers);
        dataCenterPrinter.printHostsCpuUtilizationAndPowerConsumption(hostList);

        //计算并打印数据中心能耗
        dataCenterPrinter.printDataCenterTotalEnergyComsumption(powerMeter);

        //打印迁移次数
        System.out.printf("Number of VM migrations: %d%n", migrationsNumber);

        //记录结束时间
        final double endSecs = TimeUtil.currentTimeSecs();
        System.out.printf("Simulation finished at %s. Execution time: %.2f seconds%n", LocalTime.now(), TimeUtil.elapsedSeconds(startSecs));
    }

    private void createCloudletsAndBrokersFromTraceFileType1() throws IOException, ClassNotFoundException {
        cloudlets = new HashSet<>(30000);
        cloudletIds = new HashSet<>(30000);
        brokers = new ArrayList<>(100000);

        //使用单一代理
        if(Constant.SINGLE_BROKER){
            broker = new DatacenterBrokerSimple(simulation);
            brokers.add(broker);
        }

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
    }


    private Cloudlet createCloudlet(final TaskEvent event) {
        final long pesNumber = positive(event.actualCpuCores(Constant.VM_PES), Constant.VM_PES);
        final double maxRamUsagePercent = positive(event.getResourceRequestForRam(), Conversion.HUNDRED_PERCENT);
        final UtilizationModelDynamic utilizationRam = new UtilizationModelDynamic(0, maxRamUsagePercent);
//        final double sizeInMB    = event.getResourceRequestForLocalDiskSpace() * Constant.VM_SIZE_MB[0] + 1;
        final double sizeInMB    = 1;   //如只研究CPU和MEM，磁盘空间不考虑的话，象征性给个1mb意思一下
        final long   sizeInBytes = (long) Math.ceil(megaBytesToBytes(sizeInMB));
        return new CloudletSimple(Constant.CLOUDLET_LENGTH, pesNumber)
            .setFileSize(sizeInBytes)
            .setOutputSize(sizeInBytes)
            .setUtilizationModelBw(UtilizationModel.NULL) //如只研究CPU和MEM，忽略BW，所以设置为null
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
                    //策略刚开始阈值会比设定值大一点，以放置虚拟机。当所有虚拟机提交到主机后，阈值就会变回设定值
                    Constant.HOST_OVER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION + 0.2);
            Log.setLevel(VmAllocationPolicy.LOGGER, Level.WARN);
            this.allocationPolicy.setUnderUtilizationThreshold(Constant.HOST_UNDER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION);
            Datacenter datacenter = new DatacenterSimple(simulation,allocationPolicy);
            datacenter
                .setSchedulingInterval(Constant.SCHEDULING_INTERVAL)
                .setHostSearchRetryDelay(Constant.HOST_SEARCH_RETRY_DELAY);
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
                    //策略刚开始阈值会比设定值大一点，以放置虚拟机。当所有虚拟机提交到主机后，阈值就会变回设定值
                    Constant.HOST_OVER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION + 0.2);
            Log.setLevel(VmAllocationPolicy.LOGGER, Level.WARN);
            this.allocationPolicy.setUnderUtilizationThreshold(Constant.HOST_UNDER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION);
            Datacenter datacenter = new DatacenterSimple(simulation,allocationPolicy);
            datacenter
                .setSchedulingInterval(Constant.SCHEDULING_INTERVAL)
                .setHostSearchRetryDelay(Constant.HOST_SEARCH_RETRY_DELAY);
            datacenters.add(datacenter);
        }
        //默认只有一个datacenter，所以只提交一个hostlist
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
//        //host创建之后的活跃状态
//        final boolean activateHost = true;
//        host.setActive(activateHost);
//
//        //当虚拟机限制多久之后会被关闭
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
        final Host host = new HostSimple(Constant.HOST_RAM[hostType], Constant.HOST_BW[hostType], Constant.HOST_STORAGE[hostType], createPesList(Constant.HOST_PES,hostType));
        host
            .setVmScheduler(new VmSchedulerTimeShared())
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

        //启用host记录历史状态
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
        Random r = new Random(System.currentTimeMillis());
        int type = r.nextInt(4);
//        return new VmSimple(Constant.VM_MIPS_M, Constant.VM_PES_M).setRam(Constant.VM_RAM_M).setBw(Constant.VM_BW[0]).setSize(Constant.VM_SIZE_MB[0]);
        Vm vm = new VmSimple(Constant.VM_MIPS[type], Constant.VM_PES).setRam(Constant.VM_RAM[type]).setBw(Constant.VM_BW[type]).setSize(Constant.VM_SIZE_MB[type]);
        vm.enableUtilizationStats();
        return vm;
    }

    public void createAndSubmitVms(DatacenterBroker broker) {
        //虚拟机闲置0.2s之后销毁
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
        if(migrationsNumber > 1){
            return;
        }

        //After the first VM starts being migrated, tracks some metrics along simulation time
        simulation.addOnClockTickListener(clock -> {
            if (clock.getTime() <= 2 || (clock.getTime() >= 11 && clock.getTime() <= 15))
                showVmAllocatedMips(vm, targetHost, clock.getTime());
        });
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
        allocationPolicy.setOverUtilizationThreshold(Constant.HOST_OVER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION);
        broker.removeOnVmsCreatedListener(info.getListener());
        vmList.forEach(vm -> showVmAllocatedMips(vm, vm.getHost(), info.getTime()));

        System.out.println();
        hostList.forEach(host -> showHostAllocatedMips(info.getTime(), host));
        System.out.println();
    }

}
