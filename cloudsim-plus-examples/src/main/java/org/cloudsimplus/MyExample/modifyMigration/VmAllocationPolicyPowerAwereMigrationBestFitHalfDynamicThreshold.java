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
    //??????host?????????????????????????????????1?????????
    public double[] getHostPredictValue(Host host,double hostCpuUtilization,double hostRamUtilization,boolean max){
        return new double[]{
            Constant.USING_GM ? 1 - mathHandler.GM11Predicting(mathHandler.reverseList(host.getCpuUtilizationHistory()),Constant.HOST_LogLength,hostCpuUtilization,max) : 1 - mathHandler.ARIMRPredicting(mathHandler.reverseList(host.getCpuUtilizationHistory()),Constant.HOST_LogLength,hostCpuUtilization,max),
            Constant.USING_GM ? 1 - mathHandler.GM11Predicting(mathHandler.reverseList(host.getRamUtilizationHistory()),Constant.HOST_LogLength,hostRamUtilization,max) : 1- mathHandler.ARIMRPredicting(mathHandler.reverseList(host.getRamUtilizationHistory()),Constant.HOST_LogLength,hostRamUtilization,max)
        };
    }

    //????????????host?????????????????????????????????????????????
    @Override
    public boolean isHostOverloaded(final Host host) {
        if(isHostRamThreshold()){
            final double hostCpuUtilization = host.getCpuPercentUtilization();
            final double hostRamUtilization = host.getRamPercentUtilization();
            boolean canUpCpuThreshold = true,canUpRamThreshold = true;
//            if(hostCpuUtilization > 1.0){
//                decreaseCpuThresHold(host,getOverUtilizationThreshold(host),hostCpuUtilization,hostRamUtilization,true);
//                return true;
//            }
//            if(hostRamUtilization > 1.0){
//                decreaseRamThresHold(host,getRamOverUtilizationThreshold(host),hostCpuUtilization,hostRamUtilization,true);
//                return true;
//            }
//            boolean CpuOverload = isCpuOverload(host,hostCpuUtilization);
//            boolean RamOverload = isRamOverload(host,hostRamUtilization);
//            if(CpuOverload && RamOverload){
//                if(canUpCpuThreshold) increaseCpuThreshold(host,getOverUtilizationThreshold(host));
//                if(canUpRamThreshold) increaseRamThresHold(host,getRamOverUtilizationThreshold(host));
////                return isHostOverloaded(host,hostCpuUtilization,hostRamUtilization);
//            }else if(!CpuOverload && RamOverload){
//                if(canUpRamThreshold) increaseRamThresHold(host,getRamOverUtilizationThreshold(host));
//                RamOverload = isRamOverload(host, hostRamUtilization);
////                return RamOverload;
//            }else if(CpuOverload){
//                if(canUpCpuThreshold) increaseCpuThreshold(host,getOverUtilizationThreshold(host));
//                CpuOverload = isCpuOverload(host, hostCpuUtilization);
////                return CpuOverload;
//            }

//            boolean currentOverload = isHostOverloaded(host,hostCpuUtilization,hostRamUtilization);
//            // U > T ,????????????????????????;U <= T,??????????????????
//            if(currentOverload){
//                return true;
//            }

            if(host.getSimulation().clock() < 1) return false;

            boolean futureOverload = false;
            //???????????????????????????U'???hostPredict[0]???cpu?????????,hostPredict[1]??????????????????
            final double[] hostPredict = getHostPredictValue(host,hostCpuUtilization,hostRamUtilization,false);
            final double predictCpuUsage = 1 - hostPredict[0],predictRamUsage = 1 - hostPredict[1];
            if(predictCpuUsage > 1.0){
                decreaseCpuThresHold(host,getOverUtilizationThreshold(host),hostCpuUtilization,hostRamUtilization,true);
                canUpCpuThreshold = false;
            }
            if(predictRamUsage > 1.0){
                decreaseRamThresHold(host,getRamOverUtilizationThreshold(host),hostCpuUtilization,hostRamUtilization,true);
                canUpRamThreshold = false;
            }
            //?????????????????????U'???T??????
            boolean futureCpuOverload = isHostOverloaded(host,predictCpuUsage,hostRamUtilization);
            boolean futureRamOverload = isHostOverloaded(host,hostCpuUtilization,predictRamUsage);
            //U' <= T ,????????????T???T???
            //cpu???ram???????????????????????????
            if(!futureCpuOverload && !futureRamOverload){
                return false;
            }else if(!futureCpuOverload){
                if(canUpRamThreshold) increaseRamThresHold(host,getRamOverUtilizationThreshold(host), hostRamUtilization);
                futureRamOverload = isHostOverloaded(host,predictCpuUsage,predictRamUsage);
                return futureRamOverload;
            }else if(!futureRamOverload){
                if(canUpCpuThreshold) increaseCpuThreshold(host,getOverUtilizationThreshold(host), hostCpuUtilization);
                futureCpuOverload = isHostOverloaded(host,predictCpuUsage,predictRamUsage);
                return futureCpuOverload;
            }else {
                //cpu???ram????????????????????????????????????????????????T
                if(canUpCpuThreshold) increaseCpuThreshold(host,getOverUtilizationThreshold(host), hostCpuUtilization);
                if(canUpRamThreshold) increaseRamThresHold(host,getRamOverUtilizationThreshold(host), hostRamUtilization);
                futureOverload = isHostOverloaded(host,predictCpuUsage,predictRamUsage);
                return futureOverload;
            }
        }else{
            return isHostOverloaded(host, host.getCpuPercentUtilization());
        }
    }


//    //????????????host?????????????????????????????????????????????
//    @Override
//    public boolean isHostOverloaded(final Host host) {
//        double cpuThreshold = getOverUtilizationThreshold(host), ramThreshold = getRamOverUtilizationThreshold(host);
//        if(isHostRamThreshold()){
//            final double hostCpuUtilization = host.getCpuPercentUtilization();
//            final double hostRamUtilization = host.getRamPercentUtilization();
//            boolean canUpCpuThreshold = true,canUpRamThreshold = true;
//            if(hostCpuUtilization > 1.0){
//                decreaseCpuThresHold(host,cpuThreshold,hostCpuUtilization,hostRamUtilization,true);
//                canUpCpuThreshold = false;
//            }
//            if(hostRamUtilization > 1.0){
//                decreaseRamThresHold(host,ramThreshold,hostCpuUtilization,hostRamUtilization,true);
//                canUpRamThreshold = false;
//            }
//            boolean currentOverload = isHostOverloaded(host,hostCpuUtilization,hostRamUtilization);
//            // U > T ,????????????????????????;U <= T,??????????????????
//            if(currentOverload){
//                return true;
//            }
//            if(host.getSimulation().clock() < 1) return false;
//            boolean futureOverload = false;
//            //???????????????????????????U'???hostPredict[0]???cpu?????????,hostPredict[1]??????????????????
//            final double[] hostPredict = getHostPredictValue(host,hostCpuUtilization,hostRamUtilization,false);
//            final double predictCpuUsage = 1 - hostPredict[0],predictRamUsage = 1 - hostPredict[1];
//            if(predictCpuUsage > 1.0){
//                decreaseCpuThresHold(host,cpuThreshold,hostCpuUtilization,hostRamUtilization,true);
//                canUpCpuThreshold = false;
//            }
//            if(predictRamUsage > 1.0){
//                decreaseRamThresHold(host,ramThreshold,hostCpuUtilization,hostRamUtilization,true);
//                canUpRamThreshold = false;
//            }
//            //?????????????????????U'???T??????
//            boolean futureCpuOverload = isHostOverloaded(host,predictCpuUsage,hostRamUtilization);
//            boolean futureRamOverload = isHostOverloaded(host,hostCpuUtilization,predictRamUsage);
//            //U' <= T ,????????????T???T???
//            //cpu???ram???????????????????????????
//            if(!futureCpuOverload && !futureRamOverload){
////                decreaseCpuThresHold(host,cpuThreshold,predictCpuUsage,predictRamUsage);
////                decreaseRamThresHold(host,ramThreshold,predictCpuUsage,predictRamUsage);
//                return false;
//            }else if(!futureCpuOverload && futureRamOverload){
//                //????????????cpu???T?????????Ram???T
//                //????????????cpu????????????????????????
////                decreaseCpuThresHold(host,cpuThreshold,predictCpuUsage,hostRamUtilization);
//                //????????????ram???T
//                if(canUpRamThreshold) increaseRamThresHold(host,ramThreshold);
//                futureRamOverload = isHostOverloaded(host,predictCpuUsage,predictRamUsage);
//                if(futureRamOverload){
//                    //????????????T??????ram??????????????????????????????????????????cpu??????????????????????????????????????????
//                    return true;
//                }else{
//                    //??????T??????ram???????????????????????????
//                    return false;
//                }
//
//            }else if(futureCpuOverload && !futureRamOverload){
//                //????????????ram???T?????????cpu???T
//                //????????????ram????????????????????????
////                decreaseRamThresHold(host,ramThreshold,hostCpuUtilization,predictRamUsage);
//                //????????????cpu????????????????????????
//                if(canUpCpuThreshold) increaseCpuThreshold(host,cpuThreshold);
//                futureCpuOverload = isHostOverloaded(host,predictCpuUsage,predictRamUsage);
//                if(futureCpuOverload){
//                    return true;
//                }else{
//                    return false;
//                }
//            }else {
//                //cpu???ram????????????????????????????????????????????????T
//                if(canUpCpuThreshold) increaseCpuThreshold(host,cpuThreshold);
//                if(canUpRamThreshold) increaseRamThresHold(host,ramThreshold);
//                futureOverload = isHostOverloaded(host,predictCpuUsage,predictRamUsage);
//                if(futureOverload){
//                    return true;
//                }else{
//                    return false;
//                }
//            }
//        }else{
//            return isHostOverloaded(host, host.getCpuPercentUtilization());
//        }
//    }

    protected boolean isNotHostOverloadedAfterAllocation(final Host host, final Vm vm) {
        final Vm tempVm = new VmSimple(vm,true);
        HostSuitability suitability = host.createTemporaryVm(tempVm);
        if (!suitability.fully()) {
            System.out.println(vm+" ???????????????"+host+"?????????????????????????????????????????????????????????????????????");
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

    public boolean isCpuOverload(final Host host, final double cpuUsagePercent)
    {
        return cpuUsagePercent > getOverUtilizationThreshold(host);
    }

    public boolean isRamOverload(final Host host, final double ramUsagePercent)
    {
        return ramUsagePercent > getRamOverUtilizationThreshold(host);
    }

    public double getModifyDistance(List<Double> usages){
//        return usages.size() >= Constant.HOST_LogLength ? mathHandler.mad(usages) : 0;
        return usages.size() >= Constant.HOST_LogLength ? mathHandler.distance6(usages) : 0;
    }

    public double getPenalityModifyDistance(List<Double> usages){
//        return usages.size() >= Constant.HOST_LogLength ? 2.5 * mathHandler.mad(mathHandler.reverseList(usages)) : 0;
        return usages.size() >= Constant.HOST_LogLength ? mathHandler.distance6(usages) : 0;
//        return getModifyDistance(usages);
    }

    public void decreaseCpuThresHold(Host host,double cpuThreshold,double CpuUsage, double RamUsage,boolean overCapacity){
        //?????????cpu???T
        double dcpu = overCapacity ? getPenalityModifyDistance(mathHandler.reverseList(host.getCpuUtilizationHistory())): getModifyDistance(mathHandler.reverseList(host.getCpuUtilizationHistory()));
        double newThreshold = Math.min(Math.max(0,1 - dcpu), 1);
        host.setCPU_THRESHOLD(newThreshold);
    }

    public void increaseCpuThreshold(Host host,double cpuThreshold, double hostCpuUtilization){
        double dcpu = getModifyDistance(mathHandler.reverseList(host.getCpuUtilizationHistory()));
        if(dcpu == 0) return;
        while(hostCpuUtilization > host.getCPU_THRESHOLD())
        {
            if(host.getCPU_THRESHOLD() + dcpu > 1 - dcpu)
            {
                host.setCPU_THRESHOLD(1 - dcpu);
                return;
            }
            double newCpuThreshold = host.getCPU_THRESHOLD() + dcpu;
            host.setCPU_THRESHOLD(newCpuThreshold);
        }
//        double dcpu = getModifyDistance(mathHandler.reverseList(host.getCpuUtilizationHistory()));
//        if(cpuThreshold + dcpu > 1 - dcpu)
//        {
//            host.setCPU_THRESHOLD(1 - dcpu);
//            return;
//        }
//        double newCpuThreshold = Math.min(cpuThreshold + dcpu, 1.0);
//        host.setCPU_THRESHOLD(newCpuThreshold);
    }

    public void recoverCpuThreadHold(Host host,double originalThreshold){
        host.setCPU_THRESHOLD(originalThreshold);
    }

    public void decreaseRamThresHold(Host host,double ramThreshold,double CpuUsage, double RamUsage,boolean overCapacity){
        double dram = overCapacity ? getPenalityModifyDistance(mathHandler.reverseList(host.getRamUtilizationHistory())): getModifyDistance(mathHandler.reverseList(host.getRamUtilizationHistory()));
        double newThreshold = Math.min(Math.max(0,1 - dram),1);
        host.setRAM_THRESHOLD(newThreshold);
    }

    public void increaseRamThresHold(Host host,double ramThreshold, double hostRamUtilization){
        double dram = getModifyDistance(mathHandler.reverseList(host.getRamUtilizationHistory()));
        if(dram == 0) return;
        while(hostRamUtilization > host.getRAM_THRESHOLD())
        {
            if(host.getRAM_THRESHOLD() + dram > 1 - dram)
            {
                host.setRAM_THRESHOLD(1 - dram);
                return;
            }
            double newRamThreshold = host.getRAM_THRESHOLD() + dram;
            host.setRAM_THRESHOLD(newRamThreshold);
        }
//        double dram = getModifyDistance(mathHandler.reverseList(host.getRamUtilizationHistory()));
//        if(ramThreshold + dram > 1 - dram)
//        {
//            host.setRAM_THRESHOLD(1 - dram);
//            return;
//        }
//        double newRamThreshold = Math.min(ramThreshold + dram, 1.0);
//        host.setRAM_THRESHOLD(newRamThreshold);
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
