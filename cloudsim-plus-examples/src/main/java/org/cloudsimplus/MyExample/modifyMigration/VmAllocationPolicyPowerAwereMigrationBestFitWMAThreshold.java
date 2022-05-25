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

public class VmAllocationPolicyPowerAwereMigrationBestFitWMAThreshold extends VmAllocationPolicyMigrationDynamicUpperThresholdFirstFit {
    private final MathHandler mathHandler;
    private final Map<Host, LinkedList<Double>> allHostsRamUtilizationHistoryQueue;
    private final Map<Host,LinkedList<Double>> allHostsCpuUtilizationHistoryQueue;
    private final Map<Vm,LinkedList<Double>> allVmsRamUtilizationHistoryQueue;
    private final Map<Vm,LinkedList<Double>> allVmsCpuUtilizationHistoryQueue;

    public VmAllocationPolicyPowerAwereMigrationBestFitWMAThreshold(
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
        return 1.0;
    }

    @Override
    public double getRamOverUtilizationThreshold(final Host host) {
        return 1.0;
    }

    @Override
    public double computeHostUtilizationMeasure(Host host) throws IllegalStateException {
        return 0;
    }

    public double getPredictedUtilization(Host host, List<Double> usages){
        return mathHandler.WMAPredicting(usages, 30,Double.MAX_VALUE, true) * getSafetyParameter();
    }

    //重写判断host本身是否过载，用未来利用率预测
    @Override
    public boolean isHostOverloaded(final Host host) {
        List<Double> cpuUsages = host.getCpuUtilizationHistory();
        List<Double> ramUsages = host.getRamUtilizationHistory();
        if(cpuUsages.size() < 30 || ramUsages.size() < 30) {
            return getFallbackVmAllocationPolicy().isHostOverloaded(host);
        }
        if(isHostRamThreshold()){
            double cpuLrPredict = getPredictedUtilization(host,cpuUsages);
            double ramLrPredict = getPredictedUtilization(host,ramUsages);
            return isHostOverloaded(host, cpuLrPredict,ramLrPredict);
        }else{
            return isHostOverloaded(host, host.getCpuPercentUtilization());
        }
    }

    protected boolean isHostOverloaded(final Host host, final double cpuUsagePercent, final double ramUsagePercent){
        return cpuUsagePercent > 1.0 || ramUsagePercent > 1.0;
    }

}
