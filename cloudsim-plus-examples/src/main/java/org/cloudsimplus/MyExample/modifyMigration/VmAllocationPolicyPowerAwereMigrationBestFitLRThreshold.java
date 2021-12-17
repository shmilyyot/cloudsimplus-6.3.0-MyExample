package org.cloudsimplus.MyExample.modifyMigration;

import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigration;
import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigrationDynamicUpperThreshold;
import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigrationDynamicUpperThresholdFirstFit;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.selectionpolicies.VmSelectionPolicy;
import org.cloudbus.cloudsim.util.Conversion;
import org.cloudbus.cloudsim.util.MathUtil;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudsimplus.MyExample.Constant;
import org.cloudsimplus.MyExample.MathHandler;

import java.util.*;
import java.util.stream.Stream;

public class VmAllocationPolicyPowerAwereMigrationBestFitLRThreshold extends VmAllocationPolicyMigrationDynamicUpperThresholdFirstFit {
    private final MathHandler mathHandler;
    private final Map<Host, LinkedList<Double>> allHostsRamUtilizationHistoryQueue;
    private final Map<Host,LinkedList<Double>> allHostsCpuUtilizationHistoryQueue;
    private final Map<Vm,LinkedList<Double>> allVmsRamUtilizationHistoryQueue;
    private final Map<Vm,LinkedList<Double>> allVmsCpuUtilizationHistoryQueue;

    public VmAllocationPolicyPowerAwereMigrationBestFitLRThreshold(
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
        final double utilization = hostPotentialMipsUse / host.getTotalMipsCapacity();
        return utilization;
    }

    public double[] getVmPredictValue(Vm vm,double vmCpuUtilization,double vmRamUtilization,boolean max){
        return new double[]{mathHandler.GM11Predicting(getVmCpuUtilizationHistory(vm), Constant.VM_LogLength,vmCpuUtilization,max),mathHandler.GM11Predicting(getVmRamUtilizationHistory(vm),Constant.VM_LogLength,vmRamUtilization,max)};
    }

    //获取host最小剩余资源利用率，用1减过了
    public double[] getHostPredictValue(Host host,double hostCpuUtilization,double hostRamUtilization,boolean max){
        return new double[]{1-mathHandler.GM11Predicting(getCpuUtilizationHistory(host),Constant.HOST_LogLength,hostCpuUtilization,max),1-mathHandler.GM11Predicting(getRamUtilizationHistory(host),Constant.HOST_LogLength,hostRamUtilization,max)};
    }

    protected double[] getParameterEstimates(double[] usages) {
        return mathHandler.getLoessParameterEstimates(usages);
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
        return 0;
    }


    @Override
    public double getRamOverUtilizationThreshold(final Host host) {
        return 0;
    }

    @Override
    public double computeHostUtilizationMeasure(Host host) throws IllegalStateException {
        return 0;
    }

    protected double getMaximumVmMigrationTime(Host host) {
        long maxRam = Long.MIN_VALUE;
        for (Vm vm : host.getVmList()) {
            long ram = vm.getCurrentRequestedRam();
            if (ram > maxRam) {
                maxRam = ram;
            }
        }
        return maxRam / (Conversion.bitesToBytes(host.getBw().getCapacity()) * 0.5);
    }

    public double getPredictedUtilization(Host host,List<Double> usages){
        double[] utilizationHistory = mathHandler.convertListToArray(usages);
        double[] utilizationHistoryReversed = new double[Constant.HOST_LogLength];
        for (int i = 0; i < Constant.HOST_LogLength; i++) {
            utilizationHistoryReversed[i] = utilizationHistory[Constant.HOST_LogLength - i - 1];
        }
        double[] estimates = null;
        try {
            estimates = getParameterEstimates(utilizationHistoryReversed);
        } catch (IllegalArgumentException e) {
            return Double.MAX_VALUE;
        }
        double migrationIntervals = Math.ceil(getMaximumVmMigrationTime(host) / Constant.SCHEDULING_INTERVAL);
        double predictedUtilization = estimates[0] + estimates[1] * (Constant.HOST_LogLength + migrationIntervals);
        predictedUtilization *= getSafetyParameter();
        return predictedUtilization;
    }

    //重写判断host本身是否过载，用未来利用率预测
    @Override
    public boolean isHostOverloaded(final Host host) {
        List<Double> cpuUsages = getCpuUtilizationHistory(host);
        List<Double> ramUsages = getRamUtilizationHistory(host);
        if(cpuUsages.size() < Constant.HOST_LogLength || ramUsages.size() < Constant.HOST_LogLength) {
            return getFallbackVmAllocationPolicy().isHostOverloaded(host);
        }
        if(isHostRamThreshold()){
            final double hostCpuUtilization = host.getCpuPercentUtilization();
            final double hostRamUtilization = host.getRamPercentUtilization();
            if(host.isInFindMigrateVm()){
                return isHostOverloaded(host, hostCpuUtilization,hostRamUtilization);
            }else{
                double cpuLrPredict = getPredictedUtilization(host,cpuUsages);
                double ramLrPredict = getPredictedUtilization(host,ramUsages);
                if(cpuLrPredict == Double.MAX_VALUE || ramLrPredict == Double.MAX_VALUE){
                    return getFallbackVmAllocationPolicy().isHostOverloaded(host);
                }
                return isHostOverloaded(host, cpuLrPredict,ramLrPredict);
            }
        }else{
            return isHostOverloaded(host, host.getCpuPercentUtilization());
        }
    }

    protected boolean isHostOverloaded(final Host host, final double cpuUsagePercent, final double ramUsagePercent){
        return cpuUsagePercent >= 1.0 && ramUsagePercent >= 1.0;
    }

    //重写判断一个vm放进去host的话会不会过载
    @Override
    protected boolean isNotHostOverloadedAfterAllocation(final Host host, final Vm vm) {
        List<Double> cpuUsages = getCpuUtilizationHistory(host);
        List<Double> ramUsages = getRamUtilizationHistory(host);
        List<Double> cpuVmUsages = getVmCpuUtilizationHistory(vm);
        List<Double> ramVmUsages = getVmRamUtilizationHistory(vm);
        if(cpuUsages.size() < Constant.HOST_LogLength || ramUsages.size() < Constant.HOST_LogLength || cpuVmUsages.size() < Constant.HOST_LogLength || ramVmUsages.size() < Constant.HOST_LogLength){
            return !getFallbackVmAllocationPolicy().isHostOverloaded(host);
        }
        for(int i=0;i < Constant.HOST_LogLength;++i){
            cpuUsages.set(i,cpuUsages.get(i) + cpuVmUsages.get(i) * vm.getTotalMipsCapacity() / host.getTotalMipsCapacity());
            ramUsages.set(i,ramUsages.get(i) + ramVmUsages.get(i) * vm.getRam().getCapacity() / host.getRam().getCapacity());
        }
        double cpuLrPredict = getPredictedUtilization(host,cpuUsages);
        double ramLrPredict = getPredictedUtilization(host,ramUsages);
        if(cpuLrPredict == Double.MAX_VALUE || ramLrPredict == Double.MAX_VALUE){
            return !getFallbackVmAllocationPolicy().isHostOverloaded(host);
        }
        return !isHostOverloaded(host,cpuLrPredict,ramLrPredict);
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

    @Override
    public boolean isHostUnderloaded(final Host host) {
        if(isHostRamThreshold()){
            final double hostCpuUtilization = host.getCpuPercentUtilization();
            final double hostRamUtilization = host.getRamPercentUtilization();
            List<Double> cpuHistory = getCpuUtilizationHistory(host);
            List<Double> ramHistory = getRamUtilizationHistory(host);
            //如果利用率历史小于12，直接迁移最不平衡的
            if(cpuHistory.size() < Constant.HOST_LogLength || ramHistory.size() < Constant.HOST_LogLength){
                return true;
            }
            double pHostCpuUtilization = Constant.USING_GM ? mathHandler.GM11Predicting(getCpuUtilizationHistory(host), Constant.HOST_LogLength,hostCpuUtilization,true): mathHandler.ARIMRPredicting(getCpuUtilizationHistory(host), Constant.HOST_LogLength,hostCpuUtilization,true);
            double pHostRamUtilization = Constant.USING_GM ? mathHandler.GM11Predicting(getRamUtilizationHistory(host), Constant.HOST_LogLength,hostRamUtilization,true): mathHandler.ARIMRPredicting(getRamUtilizationHistory(host), Constant.HOST_LogLength,hostRamUtilization,true);
            return isHostUnderloaded(host.getCpuPercentUtilization(),host.getRamPercentUtilization(),pHostCpuUtilization,pHostRamUtilization);
        }else{
            return isHostUnderloaded(host.getCpuPercentUtilization());
        }
    }

    public boolean isHostUnderloaded(final double cpuUsagePercent,final double ramUsagePercent,final double pHostCpuUtilization,final double pHostRamUtilization) {
        return pHostCpuUtilization <= cpuUsagePercent && pHostRamUtilization <= ramUsagePercent;
    }


}
