package org.cloudsimplus.MyExample.modifyMigration;

import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigration;
import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigrationDynamicUpperThreshold;
import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigrationDynamicUpperThresholdFirstFit;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.selectionpolicies.VmSelectionPolicy;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudsimplus.MyExample.Constant;
import org.cloudsimplus.MyExample.MathHandler;

import java.util.*;
import java.util.stream.Stream;

public class VmAllocationPolicyPowerAwereMigrationBestFitMyVersionMADThreshold extends VmAllocationPolicyMigrationDynamicUpperThresholdFirstFit {
    private final MathHandler mathHandler;
    private final Map<Host, LinkedList<Double>> allHostsRamUtilizationHistoryQueue;
    private final Map<Host,LinkedList<Double>> allHostsCpuUtilizationHistoryQueue;
    private final Map<Vm,LinkedList<Double>> allVmsRamUtilizationHistoryQueue;
    private final Map<Vm,LinkedList<Double>> allVmsCpuUtilizationHistoryQueue;

    public VmAllocationPolicyPowerAwereMigrationBestFitMyVersionMADThreshold(
        final VmSelectionPolicy vmSelectionPolicy,
        final double parameter,
        final VmAllocationPolicyMigration fallbackVmAllocationPolicy,
        final MathHandler mathHandler,
        final Map<Host, LinkedList<Double>> allHostsRamUtilizationHistoryQueue,
        final Map<Host,LinkedList<Double>> allHostsCpuUtilizationHistoryQueue,
        final Map<Vm,LinkedList<Double>> allVmsRamUtilizationHistoryQueue,
        final Map<Vm,LinkedList<Double>> allVmsCpuUtilizationHistoryQueue) {
        super(vmSelectionPolicy,parameter,fallbackVmAllocationPolicy);
        this.mathHandler = mathHandler;
        this.allHostsRamUtilizationHistoryQueue = allHostsRamUtilizationHistoryQueue;
        this.allHostsCpuUtilizationHistoryQueue = allHostsCpuUtilizationHistoryQueue;
        this.allVmsRamUtilizationHistoryQueue = allVmsRamUtilizationHistoryQueue;
        this.allVmsCpuUtilizationHistoryQueue = allVmsCpuUtilizationHistoryQueue;
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
//        final double[] vmPredict = new double[]{vmCpuUtilization,vmRamUtilization};
        return hostStream.min(Comparator.comparingDouble(host->{
            final double hostCpuUtilization = host.getCpuPercentUtilization();
            final double hostRamUtilization = host.getRamPercentUtilization();
//            final double[] hostPredict = getHostPredictValue(host,hostCpuUtilization,hostRamUtilization,true);
            final double[] hostPredict = new double[]{1-hostCpuUtilization,1-hostRamUtilization};
            return getPowerDifferenceAfterAllocation(host, vm,1-hostPredict[0],vmPredict[0]);
        }));
//        final Comparator<Host> hostPowerConsumptionComparator =
//            comparingDouble(host -> getPowerDifferenceAfterAllocation(host, vm));
//        return hostStream.min(hostPowerConsumptionComparator);
    }

    //???????????????????????????????????????
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
        final double utilization = hostPotentialMipsUse / host.getTotalMipsCapacity();
        return utilization;
    }

    public double[] getVmPredictValue(Vm vm,double vmCpuUtilization,double vmRamUtilization,boolean max){
        return new double[]{
            Constant.USING_GM ? mathHandler.GM11Predicting(mathHandler.reverseList(vm.getUtilizationHistory()), Constant.VM_LogLength,vmCpuUtilization,max): mathHandler.ARIMRPredicting(mathHandler.reverseList(vm.getUtilizationHistory()), Constant.VM_LogLength,vmCpuUtilization,max),
            Constant.USING_GM ? mathHandler.GM11Predicting(mathHandler.reverseList(vm.getUtilizationHistoryRam()),Constant.VM_LogLength,vmRamUtilization,max) : mathHandler.ARIMRPredicting(mathHandler.reverseList(vm.getUtilizationHistoryRam()),Constant.VM_LogLength,vmRamUtilization,max)
        };
    }
    //??????host?????????????????????????????????1?????????
    public double[] getHostPredictValue(Host host,double hostCpuUtilization,double hostRamUtilization,boolean max){
        return new double[]{
            Constant.USING_GM ? 1 - mathHandler.GM11Predicting(mathHandler.reverseList(host.getCpuUtilizationHistory()),Constant.HOST_LogLength,hostCpuUtilization,max) : 1 - mathHandler.ARIMRPredicting(mathHandler.reverseList(host.getCpuUtilizationHistory()),Constant.HOST_LogLength,hostCpuUtilization,max),
            Constant.USING_GM ? 1 - mathHandler.GM11Predicting(mathHandler.reverseList(host.getRamUtilizationHistory()),Constant.HOST_LogLength,hostRamUtilization,max) : 1- mathHandler.ARIMRPredicting(mathHandler.reverseList(host.getRamUtilizationHistory()),Constant.HOST_LogLength,hostRamUtilization,max)
        };
    }

    protected double getHostUtilizationMad(Host host, List<Double> usages) throws IllegalArgumentException {
        if(usages.size() >= Constant.HOST_LogLength){
            return mathHandler.mad(usages);
        }
        throw new IllegalArgumentException();
    }

    /**
     * Gets a dynamically computed Host over utilization threshold based on the
     * Host CPU utilization history.
     *
     * @param host {@inheritDoc}
     * @return {@inheritDoc} or {@link Double#MAX_VALUE} if the threshold could not be computed
     * (for instance, because the Host doesn't have enough history to use)
     * @see VmAllocationPolicyMigrationDynamicUpperThreshold#computeHostUtilizationMeasure(Host)
     */
    @Override
    public double getOverUtilizationThreshold(final Host host) {
        List<Double> usages = host.getCpuUtilizationHistory();
        if(usages.size() < Constant.HOST_LogLength){
            return getFallbackVmAllocationPolicy().getOverUtilizationThreshold(host);
        }
        try {
            return Math.min(Math.max(1 - getSafetyParameter() * getHostUtilizationMad(host,usages),0),1);
        } catch (IllegalArgumentException e) {
            return Double.MAX_VALUE;
        }
    }

    @Override
    public double getRamOverUtilizationThreshold(final Host host) {
        List<Double> usages = host.getRamUtilizationHistory();
        if(usages.size() < Constant.HOST_LogLength){
            return getFallbackVmAllocationPolicy().getOverUtilizationThreshold(host);
        }
        try {
            return Math.min(Math.max(1 - getSafetyParameter() * getHostUtilizationMad(host,usages),0),1);
        } catch (IllegalArgumentException e) {
            return Double.MAX_VALUE;
        }
    }

    @Override
    public double computeHostUtilizationMeasure(Host host) throws IllegalStateException {
        return 0;
    }

    //????????????host?????????????????????????????????????????????
    @Override
    public boolean isHostOverloaded(final Host host) {
        double cpuThreshold = getOverUtilizationThreshold(host), ramThreshold = getRamOverUtilizationThreshold(host);
        if(cpuThreshold == Double.MAX_VALUE || ramThreshold == Double.MAX_VALUE) {
            return getFallbackVmAllocationPolicy().isHostOverloaded(host);
        }
        if(isHostRamThreshold()){
            final double hostCpuUtilization = host.getCpuPercentUtilization();
            final double hostRamUtilization = host.getRamPercentUtilization();
            if(host.isInFindMigrateVm()){
                return isHostOverloaded(host, hostCpuUtilization,hostRamUtilization);
            }else{
                final double[] hostPredict = getHostPredictValue(host,hostCpuUtilization,hostRamUtilization,false);
                return isHostOverloaded(host,1-hostPredict[0],1-hostPredict[1],cpuThreshold,ramThreshold);
            }
        }else{
            return isHostOverloaded(host, host.getCpuPercentUtilization());
        }
    }

    //??????????????????vm?????????host?????????????????????
    @Override
    protected boolean isNotHostOverloadedAfterAllocation(final Host host, final Vm vm) {
        final double hostCpuUtilization = host.getCpuPercentUtilization();
        final double hostRamUtilization = host.getRamPercentUtilization();
//        System.out.println(host+" "+host.getCpuPercentUtilization()+" "+vm);
        double vmCpuUtilization = 1.0;
        double vmRamUtilization = 1.0;
        if(vm.getSimulation().clock() > 0.2){
            vmCpuUtilization = vm.getCpuPercentUtilization();
            vmRamUtilization = vm.getRam().getPercentUtilization();
        }
        final double[] vmPredict = getVmPredictValue(vm,vmCpuUtilization,vmRamUtilization,true);
        final double[] hostPredict = getHostPredictValue(host,hostCpuUtilization,hostRamUtilization,true);
        final double hostTotalCpuUsage = vmPredict[0] * vm.getTotalMipsCapacity() + (1-hostPredict[0]) * host.getTotalMipsCapacity();
        final double hostTotalRamUsage = vmPredict[1] * vm.getRam().getCapacity() + (1-hostPredict[1]) * host.getRam().getCapacity();
        final double hostCpuPredictUtilization = hostTotalCpuUsage/host.getTotalMipsCapacity();
        final double hostRamPredictUtilization = hostTotalRamUsage/host.getRam().getCapacity();
//        System.out.println(host+" "+host.getCpuPercentUtilization()+" "+vm+" "+!isHostOverloaded(host,hostCpuPredictUtilization,hostRamPredictUtilization));
        return !isHostOverloaded(host,hostCpuPredictUtilization,hostRamPredictUtilization);
    }

    protected boolean isHostOverloaded(final Host host, final double cpuUsagePercent, final double ramUsagePercent,final double cpuThreshold,final double ramThreshold){
        return cpuUsagePercent > cpuThreshold || ramUsagePercent > ramThreshold;
    }
}
