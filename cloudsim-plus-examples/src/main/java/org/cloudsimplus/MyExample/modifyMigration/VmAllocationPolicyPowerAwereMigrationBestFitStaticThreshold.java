package org.cloudsimplus.MyExample.modifyMigration;

import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicy;
import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigrationStaticThreshold;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.selectionpolicies.VmSelectionPolicy;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudsimplus.MyExample.Constant;
import org.cloudsimplus.MyExample.MathHandler;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Comparator.comparingDouble;

public class VmAllocationPolicyPowerAwereMigrationBestFitStaticThreshold extends VmAllocationPolicyMigrationStaticThreshold {
    private MathHandler mathHandler;
    private Map<Host, LinkedList<Double>> allHostsRamUtilizationHistoryQueue;
    private Map<Host,LinkedList<Double>> allHostsCpuUtilizationHistoryQueue;
    private Map<Vm,LinkedList<Double>> allVmsRamUtilizationHistoryQueue;
    private Map<Vm,LinkedList<Double>> allVmsCpuUtilizationHistoryQueue;

    public VmAllocationPolicyPowerAwereMigrationBestFitStaticThreshold(final VmSelectionPolicy vmSelectionPolicy) {
        this(vmSelectionPolicy, DEF_OVER_UTILIZATION_THRESHOLD);
    }

    public VmAllocationPolicyPowerAwereMigrationBestFitStaticThreshold(
        final VmSelectionPolicy vmSelectionPolicy,
        final double overUtilizationThreshold)
    {
        this(vmSelectionPolicy, overUtilizationThreshold, null);
    }
    public VmAllocationPolicyPowerAwereMigrationBestFitStaticThreshold(
        final VmSelectionPolicy vmSelectionPolicy,
        final double overUtilizationThreshold,
        final MathHandler mathHandler,
        final Map<Host,LinkedList<Double>> allHostsRamUtilizationHistoryQueue,
        final Map<Host,LinkedList<Double>> allHostsCpuUtilizationHistoryQueue,
        final Map<Vm,LinkedList<Double>> allVmsRamUtilizationHistoryQueue,
        final Map<Vm,LinkedList<Double>> allVmsCpuUtilizationHistoryQueue)
    {
        this(vmSelectionPolicy, overUtilizationThreshold, null);
        this.mathHandler = mathHandler;
        this.allHostsRamUtilizationHistoryQueue = allHostsRamUtilizationHistoryQueue;
        this.allHostsCpuUtilizationHistoryQueue = allHostsCpuUtilizationHistoryQueue;
        this.allVmsRamUtilizationHistoryQueue = allVmsRamUtilizationHistoryQueue;
        this.allVmsCpuUtilizationHistoryQueue = allVmsCpuUtilizationHistoryQueue;
    }

    public VmAllocationPolicyPowerAwereMigrationBestFitStaticThreshold(
        final VmSelectionPolicy vmSelectionPolicy,
        final double overUtilizationThreshold,
        final BiFunction<VmAllocationPolicy, Vm, Optional<Host>> findHostForVmFunction)
    {
        super(vmSelectionPolicy, overUtilizationThreshold, findHostForVmFunction);
    }

    @Override
    protected Optional<Host> findHostForVmInternal(final Vm vm, final Stream<Host> hostStream){
        double vmCpuUtilization = 1.0;
        double vmRamUtilization = 1.0;
        if(vm.getSimulation().clock() > 0.2){
            vmCpuUtilization = vm.getCpuPercentUtilization();
            vmRamUtilization = vm.getRam().getPercentUtilization();
        }
        final double[] vmPredict = getVmPredictValue(vm,vmCpuUtilization,vmRamUtilization,true);
        return hostStream.min(Comparator.comparingDouble(host->{
            final double hostCpuUtilization = host.getCpuPercentUtilization();
            final double hostRamUtilization = host.getRamPercentUtilization();
            final double[] hostPredict = getHostPredictValue(host,hostCpuUtilization,hostRamUtilization,true);
            return getPowerDifferenceAfterAllocation(host, vm,1-hostPredict[0],vmPredict[0]);
        }));
//        final Comparator<Host> hostPowerConsumptionComparator =
//            comparingDouble(host -> getPowerDifferenceAfterAllocation(host, vm));
//        return hostStream.min(hostPowerConsumptionComparator);
    }

    //重写计算未来放置能耗的函数
    protected double getPowerDifferenceAfterAllocation(final Host host, final Vm vm,double hostCpuUtilization,double vmCpuUtilization){
        final double powerAfterAllocation = getPowerAfterAllocation(host, vm,hostCpuUtilization,vmCpuUtilization);
        if (powerAfterAllocation > 0) {
            return powerAfterAllocation - host.getPowerModel().getPower();
        }

        return 0;
    }

    protected double getPowerAfterAllocation(final Host host, final Vm vm,double hostCpuUtilization,double vmCpuUtilization) {
        try {
            return host.getPowerModel().getPower(getMaxUtilizationAfterAllocation(host, vm,hostCpuUtilization,vmCpuUtilization));
        } catch (IllegalArgumentException e) {
            LOGGER.error("Power consumption for {} could not be determined: {}", host, e.getMessage());
        }
        return 0;
    }

    protected double getMaxUtilizationAfterAllocation(final Host host, final Vm vm,double hostCpuUtilization,double vmCpuUtilization) {
        final double requestedTotalMips = vmCpuUtilization * vm.getTotalMipsCapacity();
        double hostUtilizationMips = Math.floor((hostCpuUtilization) * host.getTotalMipsCapacity());
        if(host.getSimulation().clock()<0.2){
            hostUtilizationMips = 0.0;
            for(Vm nvm:host.getVmList()){
                hostUtilizationMips += nvm.getTotalMipsCapacity();
            }
        }
        final double hostPotentialMipsUse = hostUtilizationMips + requestedTotalMips;
        return hostPotentialMipsUse / host.getTotalMipsCapacity();
    }

    public double[] getVmPredictValue(Vm vm,double vmCpuUtilization,double vmRamUtilization,boolean max){
        return new double[]{mathHandler.GM11Predicting(getVmCpuUtilizationHistory(vm), Constant.VM_LogLength,vmCpuUtilization,max),mathHandler.GM11Predicting(getVmRamUtilizationHistory(vm),Constant.VM_LogLength,vmRamUtilization,max)};
    }

    //获取host最小剩余资源利用率，用1减过了
    public double[] getHostPredictValue(Host host,double hostCpuUtilization,double hostRamUtilization,boolean max){
        return new double[]{1-mathHandler.GM11Predicting(getCpuUtilizationHistory(host),Constant.HOST_LogLength,hostCpuUtilization,max),1-mathHandler.GM11Predicting(getRamUtilizationHistory(host),Constant.HOST_LogLength,hostRamUtilization,max)};
    }

    //重写判断host本身是否过载，用未来利用率预测
    @Override
    public boolean isHostOverloaded(final Host host) {
        if(isHostRamThreshold()){
            final double hostCpuUtilization = host.getCpuPercentUtilization();
            final double hostRamUtilization = host.getRamPercentUtilization();
            if(host.isInFindMigrateVm()){
                return isHostOverloaded(host, hostCpuUtilization,hostRamUtilization);
            }else{
                final double[] hostPredict = getHostPredictValue(host,hostCpuUtilization,hostRamUtilization,false);
                return isHostOverloaded(host, 1-hostPredict[0],1-hostPredict[1]);
            }
        }else{
            return isHostOverloaded(host, host.getCpuPercentUtilization());
        }
    }

    //重写判断一个vm放进去host的话会不会过载
    @Override
    protected boolean isNotHostOverloadedAfterAllocation(final Host host, final Vm vm) {
        final double hostCpuUtilization = host.getCpuPercentUtilization();
        final double hostRamUtilization = host.getRamPercentUtilization();
        final double vmCpuUtilization = vm.getCpuPercentUtilization();
        final double vmRamUtilization = vm.getCloudletScheduler().getCurrentRequestedRamPercentUtilization();
        final double[] vmPredict = getVmPredictValue(vm,vmCpuUtilization,vmRamUtilization,true);
        final double[] hostPredict = getHostPredictValue(host,hostCpuUtilization,hostRamUtilization,true);
        final double hostTotalCpuUsage = vmPredict[0] * vm.getTotalMipsCapacity() + (1-hostPredict[0]) * host.getTotalMipsCapacity();
        final double hostTotalRamUsage = vmPredict[1] * vm.getRam().getCapacity() + (1-hostPredict[1]) * host.getRam().getCapacity();
        final double hostCpuPredictUtilization = hostTotalCpuUsage/host.getTotalMipsCapacity();
        final double hostRamPredictUtilization = hostTotalRamUsage/host.getRam().getCapacity();
        return !isHostOverloaded(host,hostCpuPredictUtilization,hostRamPredictUtilization);
    }

    protected List<Double> getCpuUtilizationHistory(Host host) {
        final double hostCpuUtilization = host.getCpuPercentUtilization();
        return getDoubles(host, hostCpuUtilization, allVmsCpuUtilizationHistoryQueue);
    }

    private List<Double> getDoubles(Host host, double hostCpuUtilization, Map<Vm, LinkedList<Double>> allVmsUtilizationHistoryQueue) {
        double[] utilizationHistory = new double[Constant.HOST_LogLength];
        double hostMips = host.getTotalMipsCapacity();
        if(host.getVmList().isEmpty()) return new LinkedList<>();
        for (Vm vm : host.getVmList()) {
            List<Double> VmUsages = new ArrayList<>(allVmsUtilizationHistoryQueue.get(vm));
            if(VmUsages.size() < Constant.HOST_LogLength){
                return new LinkedList<>();
            }
            for (int i = 1; i < VmUsages.size(); i++) {
                utilizationHistory[i-1] += VmUsages.get(i) * vm.getTotalMipsCapacity() / hostMips;
            }
        }
        utilizationHistory[Constant.HOST_LogLength-1] = hostCpuUtilization;
        LinkedList<Double> usages = new LinkedList<>();
        for(double num:utilizationHistory){
            usages.addLast(num);
        }
        return usages;
    }

    protected List<Double> getRamUtilizationHistory(Host host) {
        final double hostRamUtilization = host.getRamPercentUtilization();
        return getDoubles(host, hostRamUtilization, allVmsRamUtilizationHistoryQueue);
    }

    protected List<Double> getVmCpuUtilizationHistory(Vm vm) {
        LinkedList<Double> cpuVmUsages = new LinkedList<>(allVmsCpuUtilizationHistoryQueue.get(vm));
        double vmCpuUtilization = vm.getCpuPercentUtilization();
        cpuVmUsages.addLast(vmCpuUtilization);
        while(cpuVmUsages.size() > Constant.VM_LogLength){
            cpuVmUsages.removeFirst();
        }
        return cpuVmUsages;
    }

    protected List<Double> getVmRamUtilizationHistory(Vm vm) {
        LinkedList<Double> ramVmUsages = new LinkedList<>(allVmsRamUtilizationHistoryQueue.get(vm));
        double vmRamUtilization = vm.getRam().getPercentUtilization();
        ramVmUsages.add(vmRamUtilization);
        while(ramVmUsages.size() > Constant.VM_LogLength){
            ramVmUsages.removeFirst();
        }
        return ramVmUsages;
    }


}
