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
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static java.util.Comparator.comparingDouble;

//默认算法就是PABFD
//VmAllocationPolicyMigrationAbstract里面的findHostForVmInternal就是根据最小的energy消耗选择的host，然后sortByCpuUtilization里面的就是按照利用率降序排序，就是pabfd
//updateMigrationMapFromUnderloadedHosts方法每次选择一个低负载的host出来迁移
//getMigrationMapFromOverloadedHosts方法可以设置过载vm找不到放置的后果
//updateMigrationMapFromUnderloadedHosts里面关闭打印欠载信息，太多了
//checkIfVmMigrationsAreNeeded()关闭打印欠载过载信息
//在hostsimple和vmsimple里面把cpuUtilizationStats关掉了，不知道有什么影响
//没有cloudlet的vm要设置为可以被迁移，空虚拟机
////大于一定时间停止迁移，在datacenter simple里面，应该可以关掉了
//vmsimple里面把iscreate注释掉了，只有在刚开始放置的时候才会返回capacity，后面都返回实际的ram利用率
//不应该按照系统间隔1秒来记录利用率变化，应该按照实际变化来记录，这样预测才有效
//请求的vm额外10%mips开销在vmschedulertimeshare
//现在是大于85392.0就停止迁移，在datacentersimple里面
//vm.getCpuPercentUtilization()基本都是没用的，都要改过来。相关使用利用率的方法都要改
//mips findhost的时候调大0.1倍好像没道理
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

    //重写查找指定host的函数
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
//            System.out.println("host:"+(1-hostPredict[0]));
//            System.out.println("vm:"+vmPredict[0]);
            double powerDifference = getPowerDifferenceAfterAllocation(host, vm,1-hostPredict[0],vmPredict[0]);
            double[] vmPredictRecourse = new double[]{vmPredict[0] * vm.getTotalMipsCapacity(),vmPredict[1] * vm.getRam().getCapacity()};
            double[] hostPredictRecourse = new double[]{hostPredict[0] * host.getTotalMipsCapacity(),hostPredict[1] * host.getRam().getCapacity()};
//            return powerDifference;
//            return mathHandler.reverseCosSimilarity(vmPredictRecourse,hostPredictRecourse);
            return powerDifference * mathHandler.reverseCosSimilarity(vmPredictRecourse,hostPredictRecourse);
//            return powerDifference / host.getVmList().size() * mathHandler.reverseCosSimilarity(vmPredictRecourse,hostPredictRecourse);
        }));
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
//        final double requestedTotalMips = vm.getCurrentUtilizationTotalMips();
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

        //用预测值和当前值之间的最大值来进行放置预测
        //只用预测值是不是也可以呢？
        //注释下面这一段就是只用预测值
        final double hostCpuUtilization = host.getCpuPercentUtilization();
        final double hostRamUtilization = host.getRamPercentUtilization();
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

        final boolean notOverloadedAfterAllocation = !isHostOverloaded(host,hostCpuPredictUtilization,hostRamPredictUtilization);

//        //只用当前值来进行判断过载
//        final boolean notOverloadedAfterAllocation = !isHostOverloaded(host);
//
////        System.out.println(vm.getId() + ": "+ host.getId() + "  "+ notOverloadedAfterAllocation);
//        host.destroyTemporaryVm(tempVm);
        return notOverloadedAfterAllocation;
    }

    public double[] getVmPredictValue(Vm vm,double vmCpuUtilization,double vmRamUtilization,boolean max){
        return new double[]{mathHandler.GM11Predicting(getVmCpuUtilizationHistory(vm), Constant.VM_LogLength,vmCpuUtilization,max),mathHandler.GM11Predicting(getVmRamUtilizationHistory(vm),Constant.VM_LogLength,vmRamUtilization,max)};
    }

    //获取host最小剩余资源利用率，用1减过了
    public double[] getHostPredictValue(Host host,double hostCpuUtilization,double hostRamUtilization,boolean max){
        return new double[]{1-mathHandler.GM11Predicting(getCpuUtilizationHistory(host),Constant.HOST_LogLength,hostCpuUtilization,max),1-mathHandler.GM11Predicting(getRamUtilizationHistory(host),Constant.HOST_LogLength,hostRamUtilization,max)};
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
