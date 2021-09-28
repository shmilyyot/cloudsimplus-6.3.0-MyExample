package org.cloudsimplus.MyExample.modifyMigration;

import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicy;
import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigrationStaticThreshold;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.selectionpolicies.VmSelectionPolicy;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudsimplus.MyExample.Constant;
import org.cloudsimplus.MyExample.MathHandler;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static java.util.Comparator.comparingDouble;

//默认算法就是PABFD
//VmAllocationPolicyMigrationAbstract里面的findHostForVmInternal就是根据最小的energy消耗选择的host，然后sortByCpuUtilization里面的就是按照利用率降序排序，就是pabfd
//updateMigrationMapFromUnderloadedHosts方法每次选择一个低负载的host出来迁移
//getMigrationMapFromOverloadedHosts方法可以设置过载vm找不到放置的后果
public class VmAllocationPolicyPASUP extends VmAllocationPolicyMigrationStaticThreshold {
    private MathHandler mathHandler;
    private Map<Host,LinkedList<Double>> allHostsRamUtilizationHistoryQueue;
    private Map<Host,LinkedList<Double>> allHostsCpuUtilizationHistoryQueue;
    private Map<Vm,LinkedList<Double>> allVmsRamUtilizationHistoryQueue;
    private Map<Vm,LinkedList<Double>> allVmsCpuUtilizationHistoryQueue;
    public VmAllocationPolicyPASUP(final VmSelectionPolicy vmSelectionPolicy)
    {
        this(vmSelectionPolicy, DEF_OVER_UTILIZATION_THRESHOLD);
    }

    public VmAllocationPolicyPASUP(
        final VmSelectionPolicy vmSelectionPolicy,
        final double overUtilizationThreshold)
    {
        this(vmSelectionPolicy, overUtilizationThreshold, null);
    }

    public VmAllocationPolicyPASUP(
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

    public VmAllocationPolicyPASUP(
        final VmSelectionPolicy vmSelectionPolicy,
        final double overUtilizationThreshold,
        final BiFunction<VmAllocationPolicy, Vm, Optional<Host>> findHostForVmFunction)
    {
        super(vmSelectionPolicy, overUtilizationThreshold, findHostForVmFunction);
    }

    private void sortByCpuUtilization(final List<? extends Vm> vmList, final double simulationTime) {
        final Comparator<Vm> comparator = comparingDouble(vm -> vm.getTotalCpuMipsUtilization(simulationTime));
        vmList.sort(comparator.reversed());
    }

    //重写查找指定host的函数
    @Override
    protected Optional<Host> findHostForVmInternal(final Vm vm, final Stream<Host> hostStream){
//        AtomicReference<Double> minFitValue = new AtomicReference<>(0.0);
//        AtomicReference<Host> targetHost = null;
        final double vmCpuUtilization = vm.getCpuPercentUtilization();
        final double vmRamUtilization = vm.getRam().getPercentUtilization();
        processCurrentVmUtilization(vm,vmCpuUtilization,vmRamUtilization);
        final double[] vmPredict = new double[]{mathHandler.DGMPredicting(allVmsCpuUtilizationHistoryQueue.get(vm)),mathHandler.DGMPredicting(allVmsRamUtilizationHistoryQueue.get(vm))};
//        hostStream.forEach(host -> {
//            double hostCpuUtilization = host.getCpuPercentUtilization();
//            double hostRamUtilization = host.getRamPercentUtilization();
//            processCurrentHostUtilization(host,hostCpuUtilization,hostRamUtilization);
//            double[] hostPredict = new double[]{mathHandler.DGMPredicting(allHostsCpuUtilizationHistoryQueue.get(host)),mathHandler.DGMPredicting(allHostsRamUtilizationHistoryQueue.get(host))};
//            double powerDifference = getPowerDifferenceAfterAllocation(host, vm);
//            double fitValue = powerDifference * mathHandler.reverseCosSimilarity(vmPredict,hostPredict);
//            if(minFitValue.get() > fitValue){
//                minFitValue.set(fitValue);
//                targetHost.set(host);
//            }
//        });
        return hostStream.min(Comparator.comparingDouble(host->{
            final double hostCpuUtilization = host.getCpuPercentUtilization();
            final double hostRamUtilization = host.getRamPercentUtilization();
            processCurrentHostUtilization(host,hostCpuUtilization,hostRamUtilization);
            final double[] hostPredict = new double[]{1-mathHandler.DGMPredicting(allHostsCpuUtilizationHistoryQueue.get(host)),1-mathHandler.DGMPredicting(allHostsRamUtilizationHistoryQueue.get(host))};
            double powerDifference = getPowerDifferenceAfterAllocation(host, vm,hostCpuUtilization,vmCpuUtilization);
            return powerDifference * mathHandler.reverseCosSimilarity(vmPredict,hostPredict);
        }));
//        return Optional.ofNullable(targetHost.get());
    }

    private void processCurrentHostUtilization(final Host host,double hostCpuUtilization,double hostRamUtilization){
        LinkedList<Double> hostRamhistory = allHostsRamUtilizationHistoryQueue.get(host);
        LinkedList<Double> hostCpuhistory = allHostsCpuUtilizationHistoryQueue.get(host);
//        if(hostRamhistory.size() >= Constant.HOST_LogLength * 2){
            while(hostRamhistory.size() > Constant.HOST_LogLength-1){
                hostRamhistory.removeFirst();
                hostCpuhistory.removeFirst();
            }
//        }
        hostRamhistory.addLast(hostRamUtilization);
        hostCpuhistory.addLast(hostCpuUtilization);
    }

    private void processCurrentVmUtilization(final Vm vm,double vmCpuUtilization,double vmRamUtilization){
        LinkedList<Double> vmRamHistory = allVmsRamUtilizationHistoryQueue.get(vm);
        LinkedList<Double> vmCpuHistory = allVmsCpuUtilizationHistoryQueue.get(vm);
//        if(vmCpuHistory.size() >= Constant.VM_LogLength * 2){
        while(vmCpuHistory.size() > Constant.VM_LogLength-1){
            vmCpuHistory.removeFirst();
            vmRamHistory.removeFirst();
        }
//        }
        vmCpuHistory.addLast(vmCpuUtilization);
        vmRamHistory.addLast(vmRamUtilization);
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
        final double hostUtilizationMips = hostCpuUtilization * host.getTotalMipsCapacity();
        final double hostPotentialMipsUse = hostUtilizationMips + requestedTotalMips;
        return hostPotentialMipsUse / host.getTotalMipsCapacity();
    }



}
