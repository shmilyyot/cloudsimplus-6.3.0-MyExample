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

        final double vmCpuUtilization = vm.getCpuPercentUtilization();
        final double vmRamUtilization = vm.getRam().getPercentUtilization();
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
            return powerDifference * mathHandler.reverseCosSimilarity(vmPredictRecourse,hostPredictRecourse);
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
            return host.getPowerModel(). getPower(getMaxUtilizationAfterAllocation(host, vm,hostCpuUtilization,vmCpuUtilization));
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
        return Math.min(utilization, 1.0);
    }

    //重写判断host本身是否过载，用未来利用率预测
    @Override
    public boolean isHostOverloaded(final Host host) {
        if(isHostRamThreshold()){
//            System.out.println(host.getCpuPercentUtilization() + "  " + host.getRamPercentUtilization());
            final double hostCpuUtilization = host.getCpuPercentUtilization();
            final double hostRamUtilization = host.getRamPercentUtilization();
//            if(hostCpuUtilization >= 1.0 || hostRamUtilization >= 1.0) host.setTotalOver100Time(host.getTotalOver100Time() + + Constant.SCHEDULING_INTERVAL);
            final double[] hostPredict = getHostPredictValue(host,hostCpuUtilization,hostRamUtilization,false);
//            System.out.println("cpu:"+(1-hostPredict[0]));
//            System.out.println("ram:"+(1-hostPredict[1]));
//            System.out.println(host.getId());
//            return isHostOverloaded(host, 1-hostPredict[0],1-hostPredict[1],hostCpuUtilization,hostRamUtilization);
            return isHostOverloaded(host, 1-hostPredict[0],1-hostPredict[1]);
//            return isHostOverloaded(host, host.getCpuPercentUtilization(),host.getRamPercentUtilization());
        }else{
            return isHostOverloaded(host, host.getCpuPercentUtilization());
        }
    }

    //对于高阈值预测，当前值和预测值在每一个维度都大于阈值才触发迁移
    protected boolean isHostOverloaded(final Host host, final double cpuUsagePercent, final double ramUsagePercent,final double hostCpuUtilization,final double hostRamUtilization){
        return (hostCpuUtilization > getOverUtilizationThreshold(host) && cpuUsagePercent > getOverUtilizationThreshold(host)) && (hostRamUtilization > getRamOverUtilizationThreshold(host) && ramUsagePercent > getRamOverUtilizationThreshold(host));
    }

    //对于高阈值预测，当前值和预测值在每一个维度都大于阈值才触发迁移
    protected boolean isHostOverloaded(final Host host, final double cpuUsagePercent, final double ramUsagePercent){
        return cpuUsagePercent > getOverUtilizationThreshold(host) && ramUsagePercent > getRamOverUtilizationThreshold(host);
    }

    //对于高阈值预测，当前值和预测值在每一个维度都大于阈值才触发迁移
    protected boolean isHostOverloadedAfter(final Host host, final double cpuUsagePercent, final double ramUsagePercent){
        return cpuUsagePercent > getOverUtilizationThreshold(host) || ramUsagePercent > getRamOverUtilizationThreshold(host);
    }

    //重写判断一个vm放进去host的话会不会过载
    @Override
    protected boolean isNotHostOverloadedAfterAllocation(final Host host, final Vm vm) {

        final Vm tempVm = new VmSimple(vm,true);
//        System.out.println("mark2: "+host+" "+host.getCpuPercentUtilization()+" "+getHostCpuPercentUtilization(host)+" "+host.getTotalAllocatedMips()+" "+host.getRam().getAllocatedResource()+ " "+tempVm.getRam().getCapacity()+" "+host.getRamProvisioner().getAllocatedResourceForVm(tempVm));
//        System.out.println("mark2: ram"+getHostRamPercentRequested(host));
        HostSuitability suitability = host.createTemporaryVm(tempVm);
        if (!suitability.fully()) {
            System.out.println(vm+" 过滤剩下的"+host+"本应该可以放进去，但是实际因为容量不足放不进去");
            System.out.println("mark:"+vm+" "+vm.getCurrentUtilizationMips()+" "+vm.getCurrentRequestedRam());
            System.out.println("mark:"+tempVm+" "+tempVm.getCurrentRequestedMips()+" "+tempVm.getRam().getCapacity());
            System.out.println("mark:"+host+" TotalAvailableMips()"+host.getTotalAvailableMips()+" allocatemips:"+host.getVmScheduler().getAllocatedMips(vm));
            System.out.println("mark:"+host+" AllocatedResourceForVm:"+host.getRamProvisioner().getAllocatedResourceForVm(vm)+" AvailableResource:"+host.getRam().getAvailableResource()+" "+host.getRamProvisioner().getAvailableResource());
            for(Vm tvm:host.getVmList()){
                System.out.println("mark2: "+host+" "+host.getRamProvisioner().getAvailableResource()+" "+tvm+" "+host.getRamProvisioner().getAllocatedResourceForVm(tvm)+" "+tvm.getCurrentRequestedRam());
            }
            System.out.println("---------------------------------");
            for(Vm tvm:host.getVmsMigratingIn()){
                System.out.println("mark2: "+host+" "+host.getRamProvisioner().getAvailableResource()+" "+tvm+" "+host.getRamProvisioner().getAllocatedResourceForVm(tvm)+" "+tvm.getCurrentRequestedRam());
            }
            return false;
        }

        //用预测值和当前值之间的最大值来进行放置预测
        //只用预测值是不是也可以呢？
        //注释下面这一段就是只用预测值
        final double hostCpuUtilization = getHostCpuPercentUtilization(host);
        final double hostRamUtilization = getHostRamPercentRequested(host);
        final double vmCpuUtilization = vm.getCpuPercentUtilization();
        final double vmRamUtilization = vm.getRam().getPercentUtilization();
        final double[] vmPredict = getVmPredictValue(vm,vmCpuUtilization,vmRamUtilization,true);
        final double[] hostPredict = getHostPredictValue(host,hostCpuUtilization,hostRamUtilization,true);
        final double hostTotalCpuUsage = vmPredict[0] * vm.getTotalMipsCapacity() + (1-hostPredict[0]) * host.getTotalMipsCapacity();
        final double hostTotalRamUsage = vmPredict[1] * vm.getRam().getCapacity() + (1-hostPredict[1]) * host.getRam().getCapacity();
        final double hostCpuPredictUtilization = hostTotalCpuUsage/host.getTotalMipsCapacity();
        final double hostRamPredictUtilization = hostTotalRamUsage/host.getRam().getCapacity();
        final boolean notOverloadedAfterAllocation = !isHostOverloaded(host,hostCpuPredictUtilization,hostRamPredictUtilization);

        //只用当前值来进行判断过载
//        final boolean notOverloadedAfterAllocation = !isHostOverloaded(host);

//        System.out.println(vm.getId() + ": "+ host.getId() + "  "+ notOverloadedAfterAllocation);
        host.destroyTemporaryVm(tempVm);
        return notOverloadedAfterAllocation;
    }

    public double[] getVmPredictValue(Vm vm,double vmCpuUtilization,double vmRamUtilization,boolean max){
//        processCurrentVmUtilization(vm,vmCpuUtilization,vmRamUtilization);
        return new double[]{mathHandler.GM11Predicting(allVmsCpuUtilizationHistoryQueue.get(vm),Constant.VM_LogLength,vmCpuUtilization,max),mathHandler.GM11Predicting(allVmsRamUtilizationHistoryQueue.get(vm),Constant.VM_LogLength,vmRamUtilization,max)};
    }

    //获取host最小剩余资源利用率，用1减过了
    public double[] getHostPredictValue(Host host,double hostCpuUtilization,double hostRamUtilization,boolean max){
//        processCurrentHostUtilization(host,hostCpuUtilization,hostRamUtilization);
        return new double[]{1-mathHandler.GM11Predicting(allHostsCpuUtilizationHistoryQueue.get(host),Constant.HOST_LogLength,hostCpuUtilization,max),1-mathHandler.GM11Predicting(allHostsRamUtilizationHistoryQueue.get(host),Constant.HOST_LogLength,hostRamUtilization,max)};
    }

    public boolean checkNewVmSuitable(Host host,Vm vm,final double hostTotalRamUsage){
        return vm.getNumberOfPes() <= host.getFreePesNumber() && hostTotalRamUsage <= host.getRam().getCapacity();
    }

    @Override
    public boolean isHostUnderloaded(final Host host) {
        if(isHostRamThreshold()){
            final double hostCpuUtilization = host.getCpuPercentUtilization();
            final double hostRamUtilization = host.getRamPercentUtilization();
            final double[] hostPredict = getHostPredictValue(host,hostCpuUtilization,hostRamUtilization,true);
//            return isHostUnderloaded(1-hostPredict[0],1-hostPredict[1],hostCpuUtilization,hostRamUtilization);
            return isHostUnderloaded(1-hostPredict[0],1-hostPredict[1]);
        }else{
            return isHostUnderloaded(host.getCpuPercentUtilization());
        }
//        return getHostCpuPercentRequested(host) < getUnderUtilizationThreshold();
    }

    //对于低阈值预测，当前值和预测值在每一个维度都小于阈值才触发迁移
    public boolean isHostUnderloaded(final double cpuUsagePercent,final double ramUsagePercent,final double hostCpuUtilization,final double hostRamUtilization) {
        return (cpuUsagePercent < getUnderUtilizationThreshold() && hostCpuUtilization < getUnderUtilizationThreshold()) && (ramUsagePercent < getUnderRamUtilizationThreshold() && hostRamUtilization < getUnderRamUtilizationThreshold());
    }

    //对于低阈值预测，当前值和预测值在每一个维度都小于阈值才触发迁移
    public boolean isHostUnderloaded(final double cpuUsagePercent,final double ramUsagePercent) {
        return cpuUsagePercent < getUnderUtilizationThreshold() && ramUsagePercent < getUnderRamUtilizationThreshold();
    }

    protected Host getUnderloadedHost(final Set<? extends Host> excludedHosts) {
        return this.getHostList().stream()
            .filter(host -> !excludedHosts.contains(host))
            .filter(Host::isActive)
            .filter(this::isHostUnderloaded)
            .filter(host -> host.getVmsMigratingIn().isEmpty() && !host.getVmList().isEmpty())
            .filter(this::notAllVmsAreMigratingOut)
            .max(comparingDouble(Host::getResourceWastage))
            .orElse(Host.NULL);
    }



}
