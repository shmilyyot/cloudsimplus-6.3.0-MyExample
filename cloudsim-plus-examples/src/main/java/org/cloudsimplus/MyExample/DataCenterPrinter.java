package org.cloudsimplus.MyExample;

import org.cloudbus.cloudsim.brokers.DatacenterBroker;
import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.hosts.HostStateHistoryEntry;
import org.cloudbus.cloudsim.power.PowerMeasurement;
import org.cloudbus.cloudsim.power.PowerMeter;
import org.cloudbus.cloudsim.power.models.PowerModel;
import org.cloudbus.cloudsim.power.models.PowerModelHost;
import org.cloudbus.cloudsim.power.models.PowerModelHostSimple;
import org.cloudbus.cloudsim.schedulers.MipsShare;
import org.cloudbus.cloudsim.util.Conversion;
import org.cloudbus.cloudsim.util.MathUtil;
import org.cloudbus.cloudsim.vms.HostResourceStats;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmResourceStats;
import org.cloudbus.cloudsim.vms.VmStateHistoryEntry;
import org.cloudsimplus.builders.tables.CloudletsTableBuilder;
import org.cloudsimplus.builders.tables.HostHistoryTableBuilder;
import org.cloudsimplus.builders.tables.TextTableColumn;
import org.cloudsimplus.listeners.CloudletVmEventInfo;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Comparator.comparingLong;

public class DataCenterPrinter {

    public DataCenterPrinter() {
    }

    public void printHostStateHistory(final Host host,Map<Double,Double> hostRamUtilizationHistory) {
        new HostHistoryTableBuilder(host,hostRamUtilizationHistory).setTitle(host.toString()).build();
    }

    public void printHostStateHistory(final Host host) {
        new HostHistoryTableBuilder(host).setTitle(host.toString()).build();
    }

    public void printHostInfo(Host host) {
        System.out.println();
        System.out.println("打印id为：" + host.getId() + "的host");
        System.out.println(host.getId() + "的cpu cores是" + host.getNumberOfPes());
        System.out.println(host.getId() + "的MEM是" + host.getRam().getCapacity());
        System.out.println(host.getId() + "的BW是" + host.getBw().getCapacity());
        System.out.println(host.getId() + "的Storoge是" + host.getStorage().getCapacity());
    }

    public void printCloudlets(final DatacenterBroker broker) {
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

    private long getVmSize(final Cloudlet cloudlet) {
        return cloudlet.getVm().getStorage().getCapacity();
    }

    private long getCloudletSizeInMB(final Cloudlet cloudlet) {
        return (long) Conversion.bytesToMegaBytes(cloudlet.getFileSize());
    }

    private void printHostsStatistics(List<Datacenter> datacenters, CloudSim simulation) {
        for (Datacenter datacenter : datacenters) {
            long currentActiveHosts = datacenter.getHostList().stream().filter(Host::isActive).count();
            System.out.printf("# %.2f: %d Active Host(s):%n", simulation.clock(), currentActiveHosts);
            datacenter
                .getHostList()
                .forEach(host -> System.out.printf("\tHost %3d | VMs: %4d | Active: %s %n", host.getId(), host.getVmList().size(), host.isActive()));
            System.out.println();
        }
    }


    public void printVmsCpuUtilizationAndPowerConsumption(List<DatacenterBroker> brokers) {
        for (DatacenterBroker broker : brokers) {
            List<Vm> vmList = broker.getVmCreatedList();
            vmList.sort(comparingLong(vm -> vm.getHost().getId()));
            for (Vm vm : vmList) {
                final PowerModelHost powerModel = vm.getHost().getPowerModel();
                final double hostStaticPower = powerModel instanceof PowerModelHostSimple ? ((PowerModelHostSimple) powerModel).getStaticPower() : 0;
                final double hostStaticPowerByVm = hostStaticPower / vm.getHost().getVmCreatedList().size();

                //VM CPU utilization relative to the host capacity
                final double vmRelativeCpuUtilization = vm.getCpuUtilizationStats().getMean() / vm.getHost().getVmCreatedList().size();
                final double vmPower = powerModel.getPower(vmRelativeCpuUtilization) - hostStaticPower + hostStaticPowerByVm; // W
                final VmResourceStats cpuStats = vm.getCpuUtilizationStats();
                System.out.printf(
                    "Vm   %2d CPU Usage Mean: %6.1f%% | Power Consumption Mean: %8.0f W%n",
                    vm.getId(), cpuStats.getMean() * 100, vmPower);
            }
        }
    }


    public void printHostsCpuUtilizationAndPowerConsumption(List<Host> hostList) {
        System.out.println();
        for (final Host host : hostList) {
            printHostCpuUtilizationAndPowerConsumption(host);
        }
        System.out.println();
    }

    public void printHostCpuUtilizationAndPowerConsumption(final Host host) {
        final HostResourceStats cpuStats = host.getCpuUtilizationStats();

        //The total Host's CPU utilization for the time specified by the map key
        final double utilizationPercentMean = cpuStats.getMean();
        final double watts = host.getPowerModel().getPower(utilizationPercentMean);
        System.out.printf(
            "Host %2d CPU Usage mean: %6.1f%% | Power Consumption mean: %8.0f W%n",
            host.getId(), utilizationPercentMean * 100, watts);
    }

    public void onUpdateCloudletProcessingListener(CloudletVmEventInfo eventInfo) {
        Cloudlet c = eventInfo.getCloudlet();
        double cpuUsage = c.getUtilizationModelCpu().getUtilization(eventInfo.getTime()) * 100;
        double ramUsage = c.getUtilizationModelRam().getUtilization(eventInfo.getTime()) * 100;
        double bwUsage = c.getUtilizationModelBw().getUtilization(eventInfo.getTime()) * 100;
        System.out.printf(
            "\t#EventListener: Time %.0f: Updated Cloudlet %d execution inside Vm %d",
            eventInfo.getTime(), c.getId(), eventInfo.getVm().getId());
        System.out.printf(
            "\tCurrent Cloudlet resource usage: CPU %3.0f%%, RAM %3.0f%%, BW %3.0f%%%n",
            cpuUsage, ramUsage, bwUsage);
        System.out.println();
    }

    public double printDataCenterTotalEnergyComsumption(PowerMeter powerMeter,boolean npa){
        double totalDataCenterEnergyConsumption = 0.0;
        int recordSize = powerMeter.getPowerMeasurements().size();
        if(!npa){
            double totalDataCenterPowerConsumption = 0.0;
            double previouspower = -1;
            double linerpower;
            for(PowerMeasurement powerMeasurement:powerMeter.getPowerMeasurements()){
                if(previouspower == -1){
                    linerpower = powerMeasurement.getTotalPower();
                }else{
                    linerpower = previouspower + (powerMeasurement.getTotalPower() - previouspower) / 2;
                }
                previouspower = powerMeasurement.getTotalPower();
                totalDataCenterPowerConsumption += linerpower;
//            System.out.println(powerMeasurement.getTotalPower());
            }
//        System.out.println("能耗统计的数量："+powerMeter.getPowerMeasurements().size());
            totalDataCenterEnergyConsumption = totalDataCenterPowerConsumption / ( 3600.0/Constant.COLLECTTIME * 1000);
            System.out.println("The total Energy Consumption in the system is : " + totalDataCenterEnergyConsumption + " kWh");
        }else{
            double G5power = Constant.HOST_G5_SPEC_POWER[Constant.HOST_G5_SPEC_POWER.length-1];
            double G4power = Constant.HOST_G4_SPEC_POWER[Constant.HOST_G4_SPEC_POWER.length-1];
            totalDataCenterEnergyConsumption += 400 * (G4power+G5power) * recordSize / ( 3600.0/Constant.COLLECTTIME * 1000);
            System.out.println("The total Energy Consumption in the system is : " + totalDataCenterEnergyConsumption + " kWh");
        }
        return totalDataCenterEnergyConsumption;
    }

    public double dataCenterTotalEnergyComsumption(Datacenter datacenter,boolean npa){
        double energy;
        if(npa){
            double G5power = Constant.HOST_G5_SPEC_POWER[Constant.HOST_G5_SPEC_POWER.length-1];
            double G4power = Constant.HOST_G4_SPEC_POWER[Constant.HOST_G4_SPEC_POWER.length-1];
            energy = 400 * (G4power+G5power) * 86400 / (3600 * 1000);
        }else{
            energy = datacenter.getPower() / (3600 * 1000);
        }
        System.out.println("The total Energy Consumption in the system is : " + energy + " kWh");
        return energy;
    }

    public void printHostsInformation(List<Host> hostList){
        for (Host host : hostList) {
            System.out.printf(
                "# Created %s with %.0f MIPS x %d PEs (%.0f total MIPS)%n",
                host, host.getMips(), host.getNumberOfPes(), host.getTotalMipsCapacity());
        }
    }

    public void printhosttest(Host host,Map<Host,Map<Double,Double>> allHostsRamUtilizationHistory){
        System.out.println(host + " RAM utilization history");
        System.out.println("----------------------------------------------------------------------------------");
        final Set<Double> timeSet = allHostsRamUtilizationHistory.get(host).keySet();
        final Map<Double, Double> hostRamUtilization = allHostsRamUtilizationHistory.get(host);
        for (final double time : timeSet) {
            System.out.printf(
                "Time: %10.1f secs | RAM Utilization: %10.2f%%%n",
                time, hostRamUtilization.get(time) * 100);
        }

        System.out.printf("----------------------------------------------------------------------------------%n%n");
    }

    public void showVmAllocatedMips(final Vm vm, final Host targetHost, final double time) {
        final String msg = String.format("# %.2f: %s in %s: total allocated", time, vm, targetHost);
        final MipsShare allocatedMips = targetHost.getVmScheduler().getAllocatedMips(vm);
//        System.out.println(vm+"  "+vm.getHost()+" "+targetHost+" "+vm.getCpuPercentUtilization()+" "+vm.getCpuUtilizationBeforeMigration()+" "+vm.getCurrentUtilizationMips()+" "+vm.getHost().getVmScheduler().getAllocatedMips(vm)+" "+targetHost.getVmScheduler().getAllocatedMips(vm));
        final String msg2 = allocatedMips.totalMips() == vm.getMips() * 0.9 ? " - reduction due to migration overhead" : "";
        System.out.printf("%s %f MIPs (divided by %d PEs)%s\n", msg, allocatedMips.totalMips(), allocatedMips.pes(), msg2);
    }

    public void showHostAllocatedMips(final double time, final Host host) {
        System.out.println("");
        System.out.printf(
            "%.2f: %s allocated %.2f MIPS from %.2f total capacity%n",
            time, host, host.getTotalAllocatedMips(), host.getTotalMipsCapacity());
    }

    /**
     * Prints the RAM and BW utilization history of a given Vm.
     */
    public void printVmUtilizationHistory(Vm vm,Map<Vm, Map<Double, Double>> allVmsRamUtilizationHistory) {
        System.out.println(vm + " RAM and BW utilization history");
        System.out.println("----------------------------------------------------------------------------------");

        //A set containing all resource utilization collected times
        final Set<Double> timeSet = allVmsRamUtilizationHistory.get(vm).keySet();

        final Map<Double, Double> vmRamUtilization = allVmsRamUtilizationHistory.get(vm);

        for (final double time : timeSet) {
            System.out.printf(
                "Time: %10.1f secs | RAM Utilization: %10.2f%% %n",
                time, vmRamUtilization.get(time) * 100);
        }

        System.out.printf("----------------------------------------------------------------------------------%n%n");
    }

    public long activeHostCount(List<Host> hostList,String time){
        long count = hostList.stream().filter(Host::isActive).count();
        System.out.println(time + ": 当前系统中活跃主机数目是： "+count);
        return count;
    }

    public void activeVmsCount(List<Host> hostList,String time){
        long count = 0;
        for(Host host:hostList){
            count += host.getVmCreatedList().size();
        }
        System.out.println(time + ": 当前系统中活跃虚拟机数目是： "+count);
    }

    public void calculateSLAV(List<Host> hostList,List<Vm> vmList,double totalEnergyCumsumption,int migrationsNumber){
        double SLAV;
        double SLATAH = 0.0;
        double PDM = 0.0;
        double ESV ;
        double ESVM ;
        for(Host host:hostList){
            double totalUpTime = host.getTotalUpTime();
            if(totalUpTime == 0.0) continue;
            BigDecimal b1 = new BigDecimal(Double.toString(host.getTotalOver100Time()));
            BigDecimal b2 = new BigDecimal(Double.toString(totalUpTime));
            SLATAH += b1.divide(b2, 8, RoundingMode.HALF_UP).doubleValue();
        }
        for(Vm vm:vmList){
            BigDecimal b1 = new BigDecimal(Double.toString(vm.getRequestUtilization()));
            BigDecimal b2 = new BigDecimal(Double.toString(vm.getTotalrequestUtilization()));
            PDM += b1.divide(b2, 8, RoundingMode.HALF_UP).doubleValue();
//            System.out.println("PDM "+PDM+" "+vm+" "+vm.getRequestUtilization()+" "+vm.getTotalrequestUtilization());

        }
        SLATAH /= hostList.size();
        PDM /= vmList.size();
        SLAV = SLATAH * PDM;
        ESV = SLAV * totalEnergyCumsumption;
        ESVM = ESV * migrationsNumber;
        System.out.println("当前系统的SLATAH是： " + SLATAH);
        System.out.println("当前系统的PDM是： " + PDM);
        System.out.println("当前系统的SLAV是： " + SLAV);
        System.out.println("当前系统的ESV是： " + ESV);
        System.out.println("当前系统的ESVM是： " + ESVM);
    }

    public void printNewSLAV(List<Host> hostList,List<Vm> vmList,double totalEnergyCumsumption,int migrationsNumber){
        double SLAV;
        double SLATAH = 0.0;
        double PDM = 0.0;
        double ESV ;
        double ESVM ;
        Map<String, Double> slaMetrics = getSlaMetrics(vmList);
//        double slaViolationTimePerHost = 0;
//        double totalTime = 0;
//        for(Host host:hostList){
//            slaViolationTimePerHost += host.getSlaViolationTimePerHost();
//            totalTime += host.getTotalUpTime();
//        }
//        BigDecimal b1 = new BigDecimal(Double.toString(slaViolationTimePerHost));
//        BigDecimal b2 = new BigDecimal(Double.toString(totalTime));
//        SLATAH = b1.divide(b2, 8, RoundingMode.HALF_UP).doubleValue();
        SLATAH = getSlaTimePerActiveHost(hostList);

//        double totalRequested = 0;
//        double totalUnderAllocatedDueToMigration = 0;
//        for(Vm vm:vmList){
//            totalRequested += vm.getTotalrequestUtilization();
//            totalUnderAllocatedDueToMigration += vm.getVmUnderAllocatedDueToMigration();
//        }
//        BigDecimal b3 = new BigDecimal(Double.toString(totalUnderAllocatedDueToMigration));
//        BigDecimal b4 = new BigDecimal(Double.toString(totalRequested));
//        PDM += b3.divide(b4, 8, RoundingMode.HALF_UP).doubleValue();
        PDM = slaMetrics.get("underallocated_migration");
//        double slaOverall = slaMetrics.get("overall");
//        double slaAverage = slaMetrics.get("average");
        SLAV = SLATAH * PDM;
        ESV = SLAV * totalEnergyCumsumption;
        ESVM = ESV * migrationsNumber;
        System.out.println("当前系统的SLATAH是： " + SLATAH);
        System.out.println("当前系统的PDM是： " + PDM);
        System.out.println("当前系统的SLAV是： " + SLAV);
        System.out.println("当前系统的ESV是： " + ESV);
        System.out.println("当前系统的ESVM是： " + ESVM);
    }

    protected double getSlaTimePerActiveHost(List<Host> hosts) {
        double slaViolationTimePerHost = 0;
        double totalTime = 0;

        for (Host host : hosts) {
            double previousTime = -1;
            double previousAllocated = 0;
            double previousRequested = 0;
            long previousAllocatedRam = 0;
            long previousRequestedRam = 0;
            boolean previousIsActive = true;
//            totalTime += host.getTotalUpTime();

            for (HostStateHistoryEntry entry : host.getStateHistory()) {
                if (previousTime != -1 && previousIsActive) {
                    double timeDiff = entry.getTime() - previousTime;
                    totalTime += timeDiff;
                    if (previousAllocated < previousRequested || previousAllocatedRam < previousRequestedRam) {
                        slaViolationTimePerHost += timeDiff;
                    }
                }

                previousAllocated = entry.getAllocatedMips();
                previousRequested = entry.getRequestedMips();
                previousAllocatedRam = entry.getAllocatedRam();
                previousRequestedRam = entry.getRequestedRam();
                previousTime = entry.getTime();
                previousIsActive = entry.isActive();
            }
        }
//        System.out.println(slaViolationTimePerHost+" "+totalTime);

        return slaViolationTimePerHost / totalTime;
    }

    protected static Map<String, Double> getSlaMetrics(List<Vm> vms) {
        Map<String, Double> metrics = new HashMap<String, Double>();
        List<Double> slaViolation = new LinkedList<Double>();
        double totalAllocated = 0;
        double totalRequested = 0;
        double totalUnderAllocatedDueToMigration = 0;

        for (Vm vm : vms) {
            double vmTotalAllocated = 0;
            double vmTotalRequested = 0;
            double vmUnderAllocatedDueToMigration = 0;
            double previousTime = -1;
            double previousAllocated = 0;
            double previousRequested = 0;
            boolean previousIsInMigration = false;

            for (VmStateHistoryEntry entry : vm.getStateHistory()) {
                if (previousTime != -1) {
                    double timeDiff = entry.getTime() - previousTime;
                    vmTotalAllocated += previousAllocated * timeDiff;
                    vmTotalRequested += previousRequested * timeDiff;

                    if (previousAllocated < previousRequested) {
                        slaViolation.add((previousRequested - previousAllocated) / previousRequested);
                        if (previousIsInMigration) {
                            vmUnderAllocatedDueToMigration += (previousRequested - previousAllocated)
                                * timeDiff;
                        }
                    }
                }

                previousAllocated = entry.getAllocatedMips();
                previousRequested = entry.getRequestedMips();
                previousTime = entry.getTime();
                previousIsInMigration = entry.isInMigration();
            }

            totalAllocated += vmTotalAllocated;
            totalRequested += vmTotalRequested;
            totalUnderAllocatedDueToMigration += vmUnderAllocatedDueToMigration;
        }

        metrics.put("overall", (totalRequested - totalAllocated) / totalRequested);
        if (slaViolation.isEmpty()) {
            metrics.put("average", 0.);
        } else {
            metrics.put("average", MathUtil.mean(slaViolation));
        }
        metrics.put("underallocated_migration", totalUnderAllocatedDueToMigration / totalRequested);

        return metrics;
    }

    public static List<Double> getTimesBeforeHostShutdown(List<Host> hosts) {
        List<Double> timeBeforeShutdown = new LinkedList<Double>();
        for (Host host : hosts) {
            boolean previousIsActive = true;
            double lastTimeSwitchedOn = 0;
            for (HostStateHistoryEntry entry : host.getStateHistory()) {
                if (previousIsActive == true && entry.isActive() == false) {
                    timeBeforeShutdown.add(entry.getTime() - lastTimeSwitchedOn);
                }
                if (previousIsActive == false && entry.isActive() == true) {
                    lastTimeSwitchedOn = entry.getTime();
                }
                previousIsActive = entry.isActive();
            }
        }
        return timeBeforeShutdown;
    }

    public void printSystemAverageResourceWastage(List<Double> resourceWastageList){
        double sum = 0.0;
        for(double num:resourceWastageList){
            sum += num;
        }
        double avg = sum/resourceWastageList.size();
//        double avg = resourceWastageList.stream().mapToDouble(Double::doubleValue).average().orElse(0D);
        System.out.println("当前系统平均的resourceWastage是： " + avg);
    }

    public void calculateAverageActiveHost(List<Long> activeHostNumber,int hostsize){
        double activeHostAverageNumber = activeHostNumber.stream().filter(num -> num != 0).mapToLong(Long::longValue).average().orElse(0D);
        double percentage = activeHostAverageNumber/(double)hostsize;
        System.out.println("当前系统平均活跃host数目是："+activeHostAverageNumber);
        System.out.println("当前系统平均活跃host的比例是："+ percentage);
    }

    public void printTtoalShutdownHostNumber(List<Host> hostList){
        List<Double> timeBeforeHostShutdown = getTimesBeforeHostShutdown(hostList);
        int totalShutdownNumber = timeBeforeHostShutdown.size();
//        int totalShutdownNumber = hostList.stream().mapToInt(Host::getShutdownNumber).sum() + 1;
        System.out.println("当前系统关闭的host总数是： "+totalShutdownNumber);
    }

}
