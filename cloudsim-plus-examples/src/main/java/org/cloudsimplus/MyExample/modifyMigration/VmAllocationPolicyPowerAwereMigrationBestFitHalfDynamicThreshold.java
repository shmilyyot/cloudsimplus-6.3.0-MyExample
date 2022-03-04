package org.cloudsimplus.MyExample.modifyMigration;

import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicy;
import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigrationStaticThreshold;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.hosts.HostSuitability;
import org.cloudbus.cloudsim.selectionpolicies.VmSelectionPolicy;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmSimple;
import org.cloudsimplus.MyExample.Constant;
import org.cloudsimplus.MyExample.MathHandler;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Stream;

public class VmAllocationPolicyPowerAwereMigrationBestFitHalfDynamicThreshold extends VmAllocationPolicyMigrationStaticThreshold {
    private MathHandler mathHandler;

    public VmAllocationPolicyPowerAwereMigrationBestFitHalfDynamicThreshold(final VmSelectionPolicy vmSelectionPolicy) {
        this(vmSelectionPolicy, DEF_OVER_UTILIZATION_THRESHOLD);
    }

    public VmAllocationPolicyPowerAwereMigrationBestFitHalfDynamicThreshold(
        final VmSelectionPolicy vmSelectionPolicy,
        final double overUtilizationThreshold)
    {
        this(vmSelectionPolicy, overUtilizationThreshold, (BiFunction<VmAllocationPolicy, Vm, Optional<Host>>) null);
    }
    public VmAllocationPolicyPowerAwereMigrationBestFitHalfDynamicThreshold(
        final VmSelectionPolicy vmSelectionPolicy,
        final double overUtilizationThreshold,
        final MathHandler mathHandler)
    {
        this(vmSelectionPolicy, overUtilizationThreshold, (BiFunction<VmAllocationPolicy, Vm, Optional<Host>>) null);
        this.mathHandler = mathHandler;
    }

    public VmAllocationPolicyPowerAwereMigrationBestFitHalfDynamicThreshold(
        final VmSelectionPolicy vmSelectionPolicy,
        final double overUtilizationThreshold,
        final BiFunction<VmAllocationPolicy, Vm, Optional<Host>> findHostForVmFunction)
    {
        super(vmSelectionPolicy, overUtilizationThreshold, findHostForVmFunction);
    }

    @Override
    protected Optional<Host> findHostForVmInternal(final Vm vm, final Stream<Host> hostStream){
        return hostStream.min(Comparator.comparingDouble(host-> getPowerDifferenceAfterAllocation(host, vm)));
    }

    public double[] getVmPredictValue(Vm vm,double vmCpuUtilization,double vmRamUtilization,boolean max){
        return new double[]{
            Constant.USING_GM ? mathHandler.GM11Predicting(mathHandler.reverseList(vm.getUtilizationHistory()), Constant.VM_LogLength,vmCpuUtilization,max): mathHandler.ARIMRPredicting(mathHandler.reverseList(vm.getUtilizationHistory()), Constant.VM_LogLength,vmCpuUtilization,max),
            Constant.USING_GM ? mathHandler.GM11Predicting(mathHandler.reverseList(vm.getUtilizationHistoryRam()),Constant.VM_LogLength,vmRamUtilization,max) : mathHandler.ARIMRPredicting(mathHandler.reverseList(vm.getUtilizationHistoryRam()),Constant.VM_LogLength,vmRamUtilization,max)
        };
    }
    //获取host最小剩余资源利用率，用1减过了
    public double[] getHostPredictValue(Host host,double hostCpuUtilization,double hostRamUtilization,boolean max){
        return new double[]{
            Constant.USING_GM ? 1 - mathHandler.GM11Predicting(mathHandler.reverseList(host.getCpuUtilizationHistory()),Constant.HOST_LogLength,hostCpuUtilization,max) : 1 - mathHandler.ARIMRPredicting(mathHandler.reverseList(host.getCpuUtilizationHistory()),Constant.HOST_LogLength,hostCpuUtilization,max),
            Constant.USING_GM ? 1 - mathHandler.GM11Predicting(mathHandler.reverseList(host.getRamUtilizationHistory()),Constant.HOST_LogLength,hostRamUtilization,max) : 1- mathHandler.ARIMRPredicting(mathHandler.reverseList(host.getRamUtilizationHistory()),Constant.HOST_LogLength,hostRamUtilization,max)
        };
    }


    //重写判断host本身是否过载，用未来利用率预测
    @Override
    public boolean isHostOverloaded(final Host host) {
        double cpuThreshold = getOverUtilizationThreshold(host), ramThreshold = getRamOverUtilizationThreshold(host);
        if(isHostRamThreshold()){
            final double hostCpuUtilization = host.getCpuPercentUtilization();
            final double hostRamUtilization = host.getRamPercentUtilization();
            boolean canUpCpuThreshold = true,canUpRamThreshold = true;
//            if(hostCpuUtilization > 1.0){
//                host.setCPU_THRESHOLD(0.9);
////                decreaseCpuThresHold(host,cpuThreshold,hostCpuUtilization,hostRamUtilization,true);
////                canUpCpuThreshold = false;
//            }
//            if(hostRamUtilization > 1.0){
//                host.setRAM_THRESHOLD(0.9);
////                decreaseRamThresHold(host,ramThreshold,hostCpuUtilization,hostRamUtilization,true);
////                canUpRamThreshold = false;
//            }
            boolean currentOverload = isHostOverloaded(host,hostCpuUtilization,hostRamUtilization);
            // U > T ,直接返回过载迁移;U <= T,继续往下执行
            if(currentOverload){
                return true;
            }
            if(host.getSimulation().clock() < 1) return false;
            boolean futureOverload = false;
            //获取主机未来利用率U'，hostPredict[0]是cpu利用率,hostPredict[1]是内存利用率
            final double[] hostPredict = getHostPredictValue(host,hostCpuUtilization,hostRamUtilization,false);
            final double predictCpuUsage = 1 - hostPredict[0],predictRamUsage = 1 - hostPredict[1];
            if(predictCpuUsage > 1.0){
//                host.setCPU_THRESHOLD(0.8);
                decreaseCpuThresHold(host,cpuThreshold,hostCpuUtilization,hostRamUtilization,true);
                canUpCpuThreshold = false;
            }
            if(predictRamUsage > 1.0){
//                host.setRAM_THRESHOLD(0.8);
                decreaseRamThresHold(host,ramThreshold,hostCpuUtilization,hostRamUtilization,true);
                canUpRamThreshold = false;
            }
            //观察未来利用率U'和T比较
            boolean futureCpuOverload = isHostOverloaded(host,predictCpuUsage,hostRamUtilization);
            boolean futureRamOverload = isHostOverloaded(host,hostCpuUtilization,predictRamUsage);
            //U' <= T ,尝试减小T为T‘
            //cpu和ram的预测值都满足条件
            if(!futureCpuOverload && !futureRamOverload){
//                decreaseCpuThresHold(host,cpuThreshold,predictCpuUsage,predictRamUsage);
//                decreaseRamThresHold(host,ramThreshold,predictCpuUsage,predictRamUsage);
                return false;
            }else if(!futureCpuOverload && futureRamOverload){
                //尝试降低cpu的T，增加Ram的T
                //尝试降低cpu的阈值，失败回退
//                decreaseCpuThresHold(host,cpuThreshold,predictCpuUsage,hostRamUtilization);
                //尝试增加ram的T
                if(canUpRamThreshold) increaseRamThresHold(host,ramThreshold);
                futureRamOverload = isHostOverloaded(host,predictCpuUsage,predictRamUsage);
                if(futureRamOverload){
                    //如果增加T之后ram依然过载，那就直接过载，哪怕cpu已经满足预测值也不过载的条件
                    return true;
                }else{
                    //增加T之后ram不过载，返回不过载
                    return false;
                }

            }else if(futureCpuOverload && !futureRamOverload){
                //尝试降低ram的T，增加cpu的T
                //尝试降低ram的阈值，失败回退
//                decreaseRamThresHold(host,ramThreshold,hostCpuUtilization,predictRamUsage);
                //尝试增加cpu的阈值，失败回退
                if(canUpCpuThreshold) increaseCpuThreshold(host,cpuThreshold);
                futureCpuOverload = isHostOverloaded(host,predictCpuUsage,predictRamUsage);
                if(futureCpuOverload){
                    return true;
                }else{
                    return false;
                }
            }else {
                //cpu和ram的预测值都大于当前值，尝试都增加T
                if(canUpCpuThreshold) increaseCpuThreshold(host,cpuThreshold);
                if(canUpRamThreshold) increaseRamThresHold(host,ramThreshold);
                futureOverload = isHostOverloaded(host,predictCpuUsage,predictRamUsage);
                if(futureOverload){
                    return true;
                }else{
                    return false;
                }
            }
        }else{
            return isHostOverloaded(host, host.getCpuPercentUtilization());
        }
    }

    protected boolean isNotHostOverloadedAfterAllocation(final Host host, final Vm vm) {
        final Vm tempVm = new VmSimple(vm,true);
        HostSuitability suitability = host.createTemporaryVm(tempVm);
        if (!suitability.fully()) {
            System.out.println(vm+" 过滤剩下的"+host+"本应该可以放进去，但是实际因为容量不足放不进去");
            return false;
        }
        final boolean notOverloadedAfterAllocation = !isHostOverloaded(host,true);
        host.destroyTemporaryVm(tempVm);
        return notOverloadedAfterAllocation;
    }

    public boolean isHostOverloaded(final Host host,final boolean inFindOverloadAfter) {
        if(isHostRamThreshold()){
            final double hostCpuUtilization = host.getCpuPercentUtilization();
            final double hostRamUtilization = host.getRamPercentUtilization();
            return isHostOverloaded(host, hostCpuUtilization,hostRamUtilization);
        }else{
            return isHostOverloaded(host, host.getCpuPercentUtilization());
        }
    }

    public double getModifyDistance(List<Double> usages){
        return usages.size() >= Constant.HOST_LogLength ? mathHandler.distance(usages) : 0;
    }

    public double getPenalityModifyDistance(List<Double> usages){
        return 2 * getModifyDistance(usages);
    }

    public void decreaseCpuThresHold(Host host,double cpuThreshold,double CpuUsage, double RamUsage,boolean overCapacity){
        //先降低cpu的T
        double dcpu = overCapacity ? getPenalityModifyDistance(mathHandler.reverseList(host.getCpuUtilizationHistory())): getModifyDistance(mathHandler.reverseList(host.getCpuUtilizationHistory()));
        double newThreshold = Math.max(0,1 - dcpu);
        host.setCPU_THRESHOLD(newThreshold);
    }

    public void increaseCpuThreshold(Host host,double cpuThreshold){
        if(host.getCPU_THRESHOLD() == 1.0) return;
        double dcpu = 1.2 * getModifyDistance(mathHandler.reverseList(host.getCpuUtilizationHistory()));
        if(cpuThreshold + dcpu > 1.0) return;
        double newCpuThreshold = Math.min(cpuThreshold + dcpu, 1.0);
        host.setCPU_THRESHOLD(newCpuThreshold);
    }

    public void recoverCpuThreadHold(Host host,double originalThreshold){
        host.setCPU_THRESHOLD(originalThreshold);
    }

    public void decreaseRamThresHold(Host host,double ramThreshold,double CpuUsage, double RamUsage,boolean overCapacity){
        double dram = overCapacity ? getPenalityModifyDistance(mathHandler.reverseList(host.getRamUtilizationHistory())): getModifyDistance(mathHandler.reverseList(host.getRamUtilizationHistory()));
        double newThreshold = Math.max(0,1 - dram);
        host.setRAM_THRESHOLD(newThreshold);
    }

    public void increaseRamThresHold(Host host,double ramThreshold){
        if(host.getRAM_THRESHOLD() == 1.0) return;
        double dram = 1.2 * getModifyDistance(mathHandler.reverseList(host.getRamUtilizationHistory()));
        if(ramThreshold + dram > 1.0) return;
        double newRamThreshold = Math.min(ramThreshold + dram, 1.0);
        host.setRAM_THRESHOLD(newRamThreshold);
    }

    public void recoverRamThreadHold(Host host ,double originalThreshold){
        host.setRAM_THRESHOLD(originalThreshold);
    }

    @Override
    public double getOverUtilizationThreshold(final Host host) {return host.getCPU_THRESHOLD();}

    @Override
    public double getRamOverUtilizationThreshold(final Host host) {
        return host.getRAM_THRESHOLD();
    }

}
