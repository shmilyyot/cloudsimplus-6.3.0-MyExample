package org.cloudsimplus.MyExample;

import org.cloudbus.cloudsim.brokers.DatacenterBroker;
import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.power.models.PowerModel;
import org.cloudbus.cloudsim.power.models.PowerModelHost;
import org.cloudbus.cloudsim.power.models.PowerModelHostSimple;
import org.cloudbus.cloudsim.util.Conversion;
import org.cloudbus.cloudsim.vms.HostResourceStats;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmResourceStats;
import org.cloudsimplus.builders.tables.CloudletsTableBuilder;
import org.cloudsimplus.builders.tables.HostHistoryTableBuilder;
import org.cloudsimplus.builders.tables.TextTableColumn;
import org.cloudsimplus.listeners.CloudletVmEventInfo;

import java.util.Comparator;
import java.util.List;

import static java.util.Comparator.comparingLong;

public class DataCenterPrinter {

    public DataCenterPrinter() {
    }

    public void printHostStateHistory(final Host host) {
        new HostHistoryTableBuilder(host).setTitle(host.toString()).build();
    }

    public void printHostInfo(Host host){
        System.out.println();
        System.out.println("打印id为："+host.getId()+"的host");
        System.out.println(host.getId()+"的cpu cores是"+host.getNumberOfPes());
        System.out.println(host.getId()+"的MEM是"+host.getRam().getCapacity());
        System.out.println(host.getId()+"的BW是"+host.getBw().getCapacity());
        System.out.println(host.getId()+"的Storoge是"+host.getStorage().getCapacity());
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

    private void printHostsStatistics(List<Datacenter> datacenters,CloudSim simulation) {
        for(Datacenter datacenter:datacenters){
            long currentActiveHosts = datacenter.getHostList().stream().filter(Host::isActive).count();
            System.out.printf("# %.2f: %d Active Host(s):%n", simulation.clock(), currentActiveHosts);
            datacenter
                .getHostList()
                .forEach(host -> System.out.printf("\tHost %3d | VMs: %4d | Active: %s %n", host.getId(), host.getVmList().size(), host.isActive()));
            System.out.println();
        }
    }


    public void printVmsCpuUtilizationAndPowerConsumption(List<DatacenterBroker> brokers) {
        for(DatacenterBroker broker:brokers){
            List<Vm> vmList = broker.getVmCreatedList();
            vmList.sort(comparingLong(vm -> vm.getHost().getId()));
            for (Vm vm : vmList) {
                final PowerModelHost powerModel = vm.getHost().getPowerModel();
                final double hostStaticPower = powerModel instanceof PowerModelHostSimple ? ((PowerModelHostSimple)powerModel).getStaticPower() : 0;
                final double hostStaticPowerByVm = hostStaticPower / vm.getHost().getVmCreatedList().size();

                //VM CPU utilization relative to the host capacity
                final double vmRelativeCpuUtilization = vm.getCpuUtilizationStats().getMean() / vm.getHost().getVmCreatedList().size();
                final double vmPower = powerModel.getPower(vmRelativeCpuUtilization) - hostStaticPower + hostStaticPowerByVm; // W
                final VmResourceStats cpuStats = vm.getCpuUtilizationStats();
                System.out.printf(
                    "Vm   %2d CPU Usage Mean: %6.1f%% | Power Consumption Mean: %8.0f W%n",
                    vm.getId(), cpuStats.getMean() *100, vmPower);
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
            "Host %2d CPU Usage mean: %6.1f%% | Power Consumption mean: %8.0f W | Total Power Consumption: null W%n",
            host.getId(), utilizationPercentMean * 100, watts);
    }

    public void onUpdateCloudletProcessingListener(CloudletVmEventInfo eventInfo) {
        Cloudlet c = eventInfo.getCloudlet();
        double cpuUsage = c.getUtilizationModelCpu().getUtilization(eventInfo.getTime())*100;
        double ramUsage = c.getUtilizationModelRam().getUtilization(eventInfo.getTime())*100;
        double bwUsage  = c.getUtilizationModelBw().getUtilization(eventInfo.getTime())*100;
        System.out.printf(
            "\t#EventListener: Time %.0f: Updated Cloudlet %d execution inside Vm %d",
            eventInfo.getTime(), c.getId(), eventInfo.getVm().getId());
        System.out.printf(
            "\tCurrent Cloudlet resource usage: CPU %3.0f%%, RAM %3.0f%%, BW %3.0f%%%n",
            cpuUsage,  ramUsage, bwUsage);
        System.out.println();
    }

}
