package org.cloudsimplus.MyExample.modifyMigration;

import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigration;
import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigrationDynamicUpperThreshold;
import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigrationDynamicUpperThresholdFirstFit;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.selectionpolicies.VmSelectionPolicy;
import org.cloudbus.cloudsim.util.MathUtil;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudsimplus.MyExample.Constant;
import org.cloudsimplus.MyExample.MathHandler;

import java.util.*;
import java.util.stream.Stream;

public class VmAllocationPolicyPowerAwereMigrationBestFitIQRThreshold extends VmAllocationPolicyMigrationDynamicUpperThresholdFirstFit {
    private final MathHandler mathHandler;
    private final Map<Host, LinkedList<Double>> allHostsRamUtilizationHistoryQueue;
    private final Map<Host,LinkedList<Double>> allHostsCpuUtilizationHistoryQueue;
    private final Map<Vm,LinkedList<Double>> allVmsRamUtilizationHistoryQueue;
    private final Map<Vm,LinkedList<Double>> allVmsCpuUtilizationHistoryQueue;

    public VmAllocationPolicyPowerAwereMigrationBestFitIQRThreshold(
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
        return hostStream.min(Comparator.comparingDouble(host-> getPowerDifferenceAfterAllocation(host, vm)));
    }

    protected double getHostUtilizationIqr(Host host,List<Double> usages) throws IllegalArgumentException {
        if(mathHandler.countNonZeroBeginning(mathHandler.convertListToArray(usages)) > 12){
            return mathHandler.iqr(usages);
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
        try {
            return Math.min(Math.max(1 - getSafetyParameter() * getHostUtilizationIqr(host,host.getCpuUtilizationHistory()),0),1);
        } catch (IllegalArgumentException e) {
            return Double.MAX_VALUE;
        }
    }

    @Override
    public double getRamOverUtilizationThreshold(final Host host) {
        try {
            return Math.max(1 - getSafetyParameter() * getHostUtilizationIqr(host,host.getRamUtilizationHistory()),0);
        } catch (IllegalArgumentException e) {
            return Double.MAX_VALUE;
        }
    }

    @Override
    public double computeHostUtilizationMeasure(Host host) throws IllegalStateException {
        return 0;
    }

    //重写判断host本身是否过载，用未来利用率预测
    @Override
    public boolean isHostOverloaded(final Host host) {
        double cpuThreshold = getOverUtilizationThreshold(host), ramThreshold = getRamOverUtilizationThreshold(host);
        if(cpuThreshold == Double.MAX_VALUE || ramThreshold == Double.MAX_VALUE) {
            return getFallbackVmAllocationPolicy().isHostOverloaded(host);
        }
        host.setCPU_THRESHOLD(cpuThreshold);
        host.setRAM_THRESHOLD(ramThreshold);
        if(isHostRamThreshold()){
            final double hostCpuUtilization = host.getCpuPercentUtilization();
            final double hostRamUtilization = host.getRamPercentUtilization();
            return isHostOverloaded(host,hostCpuUtilization,hostRamUtilization);
        }else{
            return isHostOverloaded(host, host.getCpuPercentUtilization());
        }
    }

}


