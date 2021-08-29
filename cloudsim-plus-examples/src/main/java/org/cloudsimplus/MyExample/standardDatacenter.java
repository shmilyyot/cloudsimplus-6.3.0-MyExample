package org.cloudsimplus.MyExample;

import ch.qos.logback.classic.Level;
import com.alibaba.fastjson.JSON;
import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.brokers.DatacenterBroker;
import org.cloudbus.cloudsim.brokers.DatacenterBrokerSimple;
import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.cloudlets.CloudletSimple;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.events.PredicateType;
import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.datacenters.DatacenterSimple;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.hosts.HostSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.ResourceProvisionerSimple;
import org.cloudbus.cloudsim.resources.Pe;
import org.cloudbus.cloudsim.resources.PeSimple;
import org.cloudbus.cloudsim.schedulers.vm.VmScheduler;
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.util.Conversion;
import org.cloudbus.cloudsim.util.TimeUtil;
import org.cloudbus.cloudsim.util.TraceReaderAbstract;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModel;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelDynamic;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelFull;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmSimple;
import org.cloudsimplus.builders.tables.CloudletsTableBuilder;
import org.cloudsimplus.builders.tables.HostHistoryTableBuilder;
import org.cloudsimplus.builders.tables.TextTableColumn;
import org.cloudsimplus.examples.traces.google.GoogleTaskEventsExample1;
import org.cloudsimplus.traces.google.*;
import org.cloudsimplus.util.Log;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalTime;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import static org.cloudbus.cloudsim.util.Conversion.megaBytesToBytes;
import static org.cloudbus.cloudsim.util.MathUtil.positive;
import java.util.Random;

/**
 * 论文实验标准数据中心模板设置
 * 本模板所有参数和功能分支皆可在同一目录下的{@link Constant}文件中修改，无需特意改动本文件
 * 若多个cloudlet分配到同一个vm，cpu资源会对半分，然后继续占各自的部分比例
 * */

public class standardDatacenter {
    /**
    * google存储文件路径
    * */
    private static List<String> TRACE_FILENAMES;
    private static List<String> Usage_FILENAMES;

    /**
     * cloudsim仿真数据中心相关设置
     * */
    private final CloudSim simulation;  //仿真启动对象
    private List<Datacenter> datacenters;   //多个数据中心
    private Collection<Cloudlet> cloudlets;    //数据中心的任务
    private List<Host> hostList;    //主机列表
    private List<DatacenterBroker> brokers;    //数据中心的多个代理
    private DatacenterBroker broker;    //数据中心的单个代理
    private Set<Long> hostIds;  //数据中心host的id
    private Set<Long> cloudletIds;  //系统中cloudlet的id

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        //重定向控制台输出到本地log
        try{
            System.setOut(new PrintStream(new FileOutputStream(Constant.LOG_FILE_PATH)));
        }catch (FileNotFoundException e){
            e.printStackTrace();
        }
        //创建所有文件路径
        buildTraceFileNames();
        //启动标准数据中心
        new standardDatacenter();
    }
    private standardDatacenter() throws IOException, ClassNotFoundException {
        //模拟日志打印，记录开始时间
        final double startSecs = TimeUtil.currentTimeSecs();
        System.out.printf("Simulation started at %s%n%n", LocalTime.now());
        Log.setLevel(Level.TRACE);

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
//        brokers.forEach(broker -> broker.submitVmList(createVms()));

//        //打印brokers和cloudlets的信息
//        System.out.println("Brokers:");
//        brokers.stream().sorted().forEach(b -> System.out.printf("\t%d - %s%n", b.getId(), b.getName()));
//        System.out.println("Cloudlets:");
//        cloudlets.stream().sorted().forEach(c -> System.out.printf("\t%s (job %d)%n", c, c.getJobId()));
//
//        //数据中心模拟器启动
//        simulation.start();
//
//        //打印所有cloudlet运行状况
//        brokers.stream().sorted().forEach(this::printCloudlets);
//        System.out.printf("Simulation finished at %s. Execution time: %.2f seconds%n", LocalTime.now(), TimeUtil.elapsedSeconds(startSecs));

//        //打印生成的服务器的配置信息
//        hostList.stream().forEach(this::printHostInfo);
//        //打印host的cpu利用率
//        System.out.printf("%nHosts CPU usage History (when the allocated MIPS is lower than the requested, it is due to VM migration overhead)%n");
//        hostList.stream().forEach(this::printHostStateHistory);

        //记录结束时间
        final double endSecs = TimeUtil.currentTimeSecs();
        System.out.printf("Simulation ended at %s%n%n", LocalTime.now());
    }

    private static void buildTraceFileNames(){
        TRACE_FILENAMES = new ArrayList<>(500);
        Usage_FILENAMES = new ArrayList<>(500);
        for(int i=0;i<=Constant.GOOGLE_EVENT_DAYS_FILE;++i){
            String filename;
            if(i<10){
                filename = Constant.TASK_EVENTS_PATH+"\\part-0000"+i+"-of-00500.csv.gz";
            }else if(i<100){
                filename = Constant.TASK_EVENTS_PATH+"\\part-000"+i+"-of-00500.csv.gz";
            }else{
                filename = Constant.TASK_EVENTS_PATH+"\\part-00"+i+"-of-00500.csv.gz";
            }
            TRACE_FILENAMES.add(filename);
        }
        for(int i=0;i<=Constant.GOOGLE_EVENTUSAGE_DAYS_FILE;++i){
            String usagename;
            if(i<10){
                usagename = Constant.TASK_USAGE_PATH+"\\part-0000"+i+"-of-00500.csv.gz";
            }else if(i<100){
                usagename = Constant.TASK_USAGE_PATH+"\\part-000"+i+"-of-00500.csv.gz";
            }else{
                usagename = Constant.TASK_USAGE_PATH+"\\part-00"+i+"-of-00500.csv.gz";
            }
            Usage_FILENAMES.add(usagename);
        }
    }

    private void createCloudletsAndBrokersFromTraceFileType1() throws IOException, ClassNotFoundException {
        cloudlets = new HashSet<>(30000000);
        cloudletIds = new HashSet<>(30000000);
        brokers = new ArrayList<>(100000);
        if(Constant.USING_EXISTANCE_CLOULETS){
            if(checkCloudletIdsExist()){
                Constant.CLOUDLETID_EXIST = true;
                reverseSerializableCloudlets();
            }
        }
        if(Constant.SINGLE_BROKER){
            broker = new DatacenterBrokerSimple(simulation);
            brokers.add(broker);
        }
        for(String TRACE_FILENAME:TRACE_FILENAMES){
            GoogleTaskEventsTraceReader reader = GoogleTaskEventsTraceReader.getInstance(simulation, TRACE_FILENAME,this::createCloudlet);
            if(Constant.USING_EXISTANCE_CLOULETS && Constant.CLOUDLETID_EXIST){
                reader.setPredicate(event -> event.getTimestamp() <= Constant.STOP_TIME && cloudletIds.contains(event.getUniqueTaskId()));
            }else{
                if(Constant.READ_INITIAL_MACHINE_CLOUDLET){
                    if(Constant.USING_GOOGLE_HOST){
                        reader.setPredicate(event -> (event.getTimestamp() <= Constant.STOP_TIME && hostIds.contains(event.getMachineId())));
                    }else{
                        reader.setPredicate(event -> (event.getTimestamp() <= Constant.STOP_TIME));
                        reader.setMaxCloudletsToCreate(200);
                    }
                }else{
                    reader.setPredicate(event -> (event.getTimestamp() <= Constant.STOP_TIME));
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
                    "TaskEventFile： %d Cloudlets created from the %s trace file and total %d Brokers.%n",
                    eachfileCloudlets.size(), TRACE_FILENAME, brokers.size());
            }
        }
        if(!Constant.CLOUDLETID_EXIST){
            cloudlets.forEach(cloudlet -> cloudletIds.add(cloudlet.getId()));
            serializableCloudlets();
        }
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
            .setUtilizationModelCpu(new UtilizationModelFull())
            .setUtilizationModelRam(utilizationRam);
    }

    private void createGoogleDatacenters() {
        datacenters = new ArrayList<>(Constant.DATACENTERS_NUMBER);
        hostIds = new HashSet<>(Constant.HOST_SIZE);
        final GoogleMachineEventsTraceReader reader = GoogleMachineEventsTraceReader.getInstance(Constant.MACHINE_FILENAME , this::createHost);
        reader.setMaxRamCapacity(Constant.MAX_HOST_MEM);
        reader.setMaxCpuCores(Constant.MAX_HOST_CORES);
        reader.setMaxLinesToRead(Constant.GOOGLE_MACHINE_LINES_FILE);
        //Creates Datacenters with no hosts.
        for(int i = 0; i < Constant.DATACENTERS_NUMBER; i++){
            datacenters.add(new DatacenterSimple(simulation, new VmAllocationPolicySimple()));
        }
        reader.setDatacenterForLaterHosts(datacenters.get(0));
        List<Host> totalReadHosts = new ArrayList<>(reader.process());
        if(Constant.NUMBER_RANDOM_HOSTS){
            hostList = randomChooseHostsFromGoogleHosts(totalReadHosts);
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
        datacenters.add(new DatacenterSimple(simulation, hostList, new VmAllocationPolicySimple()));
    }

    private Host createHost(final MachineEvent event) {
        final Host host = new HostSimple(event.getRam(), Constant.MAX_HOST_BW, Constant.MAX_HOST_STORAGE, createPesList(event.getCpuCores()))
            .setVmScheduler(new VmSchedulerTimeShared())
            .setRamProvisioner(new ResourceProvisionerSimple())
            .setBwProvisioner(new ResourceProvisionerSimple());
        host.setId(event.getMachineId());
        hostIds.add(host.getId());
        host.enableStateHistory();
        return host;
    }
    private Host createHost(int hostType) {
        final Host host = new HostSimple(Constant.HOST_RAM[hostType], Constant.HOST_BW[hostType], Constant.HOST_STORAGE[hostType], createPesList(Constant.HOST_PES,hostType))
            .setVmScheduler(new VmSchedulerTimeShared())
            .setRamProvisioner(new ResourceProvisionerSimple())
            .setBwProvisioner(new ResourceProvisionerSimple());
        host.enableStateHistory();
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

    private void printHostStateHistory(final Host host) {
        new HostHistoryTableBuilder(host).setTitle(host.toString()).build();
    }

    public List<Host> randomChooseHostsFromGoogleHosts(List<Host> hostList){
        int trueSize = Math.min(hostList.size(), Constant.HOST_SIZE);
        int totalHostNumber = hostList.size();
        Random r = new Random(System.currentTimeMillis());
        List<Host> randomHostList = new ArrayList<>(trueSize);
        Set<Integer> pickedHostIdPos = new HashSet<>(trueSize);
        Set<Long> randomHostIds = new HashSet<>(trueSize);
        while(pickedHostIdPos.size()<trueSize){
            int randomHostPos = r.nextInt(totalHostNumber);
            pickedHostIdPos.add(randomHostPos);
        }
        for(int hostIdPos:pickedHostIdPos){
            Host host = hostList.get(hostIdPos);
            randomHostList.add(host);
            randomHostIds.add(host.getId());
        }
        hostIds = randomHostIds;
        return randomHostList;
    }

    private void readTaskUsageTraceFile() {
        Set<Long> illegalCloudedIds = new HashSet<>();
        for(String eachFileUsageName:Usage_FILENAMES){
            GoogleTaskUsageTraceReader reader = GoogleTaskUsageTraceReader.getInstance(brokers, eachFileUsageName);
            if(Constant.FILTER_INSIDE_CLOUDLET){
                reader.setPredicate(taskUsage ->
                    cloudletIds.contains(taskUsage.getUniqueTaskId()) &&
                        (taskUsage.getMeanCpuUsageRate()>0.05 && taskUsage.getMeanCpuUsageRate()<0.9) &&
                            (taskUsage.getCanonicalMemoryUsage()>0.05 && taskUsage.getCanonicalMemoryUsage()<0.9));
            }else{
                reader.setPredicate(taskUsage ->{
                    if(!cloudletIds.contains(taskUsage.getUniqueTaskId())) return false;
                    if(taskUsage.getMeanCpuUsageRate()<=0.05 || taskUsage.getMeanCpuUsageRate()>=0.9 ||
                        taskUsage.getCanonicalMemoryUsage()<=0.05 || taskUsage.getCanonicalMemoryUsage()>=0.9){
                            illegalCloudedIds.add(taskUsage.getUniqueTaskId());
                            cloudletIds.remove(taskUsage.getUniqueTaskId());
                            return false;
                    }
                    return true;
                });
            }
            final Collection<Cloudlet> processedCloudlets = reader.process();
            System.out.printf("TraceFile： %d Cloudlets processed from the %s trace file.%n", processedCloudlets.size(), eachFileUsageName);
        }
        if(!Constant.FILTER_INSIDE_CLOUDLET){
            cloudlets.removeIf(cloudlet -> illegalCloudedIds.contains(cloudlet.getId()));
            System.out.printf("Total %d Cloudlets and %d Brokers created!%n", cloudlets.size(),brokers.size());
        }
    }

    private List<Vm> createVms() {
        return IntStream.range(0, Constant.VMS).mapToObj(this::createVm).collect(Collectors.toList());
    }

    private Vm createVm(final int id) {
        //Uses a CloudletSchedulerTimeShared by default
        return new VmSimple(Constant.VM_MIPS[0], Constant.VM_PES).setRam(Constant.VM_RAM[0]).setBw(Constant.VM_BW[0]).setSize(Constant.VM_SIZE_MB[0]);
    }

    private long getVmSize(final Cloudlet cloudlet) {
        return cloudlet.getVm().getStorage().getCapacity();
    }

    private long getCloudletSizeInMB(final Cloudlet cloudlet) {
        return (long)Conversion.bytesToMegaBytes(cloudlet.getFileSize());
    }

    public void printHostInfo(Host host){
        System.out.println();
        System.out.println("打印id为："+host.getId()+"的host");
        System.out.println(host.getId()+"的cpu cores是"+host.getNumberOfPes());
        System.out.println(host.getId()+"的MEM是"+host.getRam().getCapacity());
        System.out.println(host.getId()+"的BW是"+host.getBw().getCapacity());
        System.out.println(host.getId()+"的Storoge是"+host.getStorage().getCapacity());
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

    private void serializableCloudlets() throws IOException{
        java.io.File file = new java.io.File(Constant.SERIAL_CLOUDLETID_PATH);
        OutputStream os = new FileOutputStream(file);
        ObjectOutputStream oos = new ObjectOutputStream(os);
        oos.writeObject(cloudletIds);
        oos.close();
        os.close();
        System.out.println("cloudlets序列化完成了！");
    }

    private void reverseSerializableCloudlets() throws IOException, ClassNotFoundException {
        java.io.File file = new java.io.File(Constant.SERIAL_CLOUDLETID_PATH);
        InputStream input = new FileInputStream(file);
        ObjectInputStream ois = new ObjectInputStream(input);
        cloudletIds = (Set<Long>)ois.readObject();
        ois.close();
        input.close();
        System.out.println("cloudlets反序列化完成了！");
    }

    private boolean checkCloudletIdsExist(){
        java.io.File file = new java.io.File(Constant.SERIAL_CLOUDLETID_PATH);
        return file.exists();
    }

}
