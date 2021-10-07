package org.cloudsimplus.MyExample.modifyMigration;

import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicy;
import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigrationStaticThreshold;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.selectionpolicies.VmSelectionPolicy;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmSimple;
import org.cloudsimplus.MyExample.Constant;
import org.cloudsimplus.MyExample.MathHandler;
import java.util.*;
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
        final double[] vmPredict = getVmPredictValue(vm);
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
//        hostStream.min(Comparator.comparingDouble(host->{
//            final double hostCpuUtilization = host.getCpuPercentUtilization();
//            final double hostRamUtilization = host.getRamPercentUtilization();
//            processCurrentHostUtilization(host,hostCpuUtilization,hostRamUtilization);
//            final double[] hostPredict = new double[]{1-mathHandler.DGMPredicting(allHostsCpuUtilizationHistoryQueue.get(host)),1-mathHandler.DGMPredicting(allHostsRamUtilizationHistoryQueue.get(host))};
//            double powerDifference = getPowerDifferenceAfterAllocation(host, vm,hostCpuUtilization,vmCpuUtilization);
//            return powerDifference * mathHandler.reverseCosSimilarity(vmPredict,hostPredict);
//        }));
//        System.out.println("finish predict vm usage");
        return hostStream.min(Comparator.comparingDouble(host->{
            final double[] hostPredict = getHostPredictValue(host);
//            System.out.println("host:"+(1-hostPredict[0]));
//            System.out.println("vm:"+vmPredict[0]);
            double powerDifference = getPowerDifferenceAfterAllocation(host, vm,1-hostPredict[0],vmPredict[0]);
            return powerDifference * mathHandler.reverseCosSimilarity(vmPredict,hostPredict);
        }));
//        return hostStream.max(Comparator.comparingDouble(Host::getCpuMipsUtilization));
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
        final double utilization = hostPotentialMipsUse / host.getTotalMipsCapacity();
        return utilization > 1 ? 1 : utilization;
    }

    //重写判断host本身是否过载，用未来利用率预测
    @Override
    public boolean isHostOverloaded(final Host host) {
        if(isHostRamThreshold()){
//            System.out.println(host.getCpuPercentUtilization() + "  " + host.getRamPercentUtilization());
            final double[] hostPredict = getHostPredictValue(host);
//            System.out.println("cpu:"+(1-hostPredict[0]));
//            System.out.println("ram:"+(1-hostPredict[1]));
//            System.out.println(host.getId());
            return isHostOverloaded(host, 1-hostPredict[0],1-hostPredict[1]);
//            return isHostOverloaded(host, host.getCpuPercentUtilization(),host.getRamPercentUtilization());
        }else{
            return isHostOverloaded(host, host.getCpuPercentUtilization());
        }
    }

    //重写判断一个vm放进去host的话会不会过载
    @Override
    protected boolean isNotHostOverloadedAfterAllocation(final Host host, final Vm vm) {

//        final double[] vmPredict = getVmPredictValue(vm);
//        final double[] hostPredict = getHostPredictValue(host);
//        final double vmCpuUsage = vmPredict[0] * vm.getTotalMipsCapacity();
//        final double hostCpuUsage = hostPredict[0] * host.getTotalMipsCapacity();
//        final double vmRamUsage = vmPredict[1] * vm.getRam().getCapacity();
//        final double hostRamUsage = hostPredict[1] * host.getRam().getCapacity();
//        final double hostTotalCpuUsage = vmCpuUsage + hostCpuUsage;
//        final double hostTotalRamUsage = vmRamUsage + hostRamUsage;
//        final double hostCpuPredictUtilization = hostTotalCpuUsage/host.getTotalMipsCapacity();
//        final double hostRamPredictUtilization = hostTotalRamUsage/host.getRam().getCapacity();
//        //删除原版复杂判断，只考虑cpu和ram，忽略迁移之类的
//        if(!checkNewVmSuitable(host,hostTotalCpuUsage,hostTotalRamUsage)){
//            return false;
//        }
//        return !isHostOverloaded(host,hostCpuPredictUtilization,hostRamPredictUtilization);

        final Vm tempVm = new VmSimple(vm);

        if (!host.createTemporaryVm(tempVm).fully()) {
            return false;
        }

        final boolean notOverloadedAfterAllocation = !isHostOverloaded(host);
//        System.out.println(vm.getId() + ": "+ host.getId() + "  "+ notOverloadedAfterAllocation);
        host.destroyTemporaryVm(tempVm);
        return notOverloadedAfterAllocation;
    }

    public double[] getVmPredictValue(Vm vm){
        final double vmCpuUtilization = vm.getCpuPercentUtilization();
        final double vmRamUtilization = vm.getRam().getPercentUtilization();
        processCurrentVmUtilization(vm,vmCpuUtilization,vmRamUtilization);
        return new double[]{mathHandler.GM11Predicting(allVmsCpuUtilizationHistoryQueue.get(vm),Constant.VM_LogLength,vmCpuUtilization,true),mathHandler.GM11Predicting(allVmsRamUtilizationHistoryQueue.get(vm),Constant.VM_LogLength,vmRamUtilization,true)};
    }

    //获取host最小剩余资源利用率，用1减过了
    public double[] getHostPredictValue(Host host){
        final double hostCpuUtilization = host.getCpuPercentUtilization();
        final double hostRamUtilization = host.getRamPercentUtilization();
        processCurrentHostUtilization(host,hostCpuUtilization,hostRamUtilization);
        return new double[]{1-mathHandler.GM11Predicting(allHostsCpuUtilizationHistoryQueue.get(host),Constant.HOST_LogLength,hostCpuUtilization,true),1-mathHandler.GM11Predicting(allHostsRamUtilizationHistoryQueue.get(host),Constant.HOST_LogLength,hostRamUtilization,true)};
    }

    public boolean checkNewVmSuitable(Host host,final double hostTotalCpuUsage,final double hostTotalRamUsage){
        return hostTotalCpuUsage <= host.getTotalMipsCapacity() && hostTotalRamUsage <= host.getRam().getCapacity();
    }

}
