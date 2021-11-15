/*
 * Title:        CloudSim Toolkit
 * Description:  CloudSim (Cloud Simulation) Toolkit for Modeling and Simulation of Clouds
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009-2012, The University of Melbourne, Australia
 */
package org.cloudbus.cloudsim.allocationpolicies.migration;

import org.apache.commons.math3.analysis.function.Constant;
import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicy;
import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicyAbstract;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.hosts.HostSuitability;
import org.cloudbus.cloudsim.provisioners.ResourceProvisioner;
import org.cloudbus.cloudsim.schedulers.MipsShare;
import org.cloudbus.cloudsim.schedulers.vm.VmScheduler;
import org.cloudbus.cloudsim.selectionpolicies.VmSelectionPolicy;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmSimple;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Comparator.comparingDouble;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * An abstract VM allocation policy that dynamically optimizes the
 * VM allocation (placement) using migration.
 * <b>It's a Best Fit policy which selects the Host with most efficient power usage to place a given VM.</b>
 * Such a behaviour can be overridden by sub-classes.
 *
 * <p>If you are using any algorithms, policies or workload included in the
 * power package please cite the following paper:
 *
 * <ul>
 * <li><a href="https://doi.org/10.1002/cpe.1867">Anton Beloglazov, and
 * Rajkumar Buyya, "Optimal Online Deterministic Algorithms and Adaptive
 * Heuristics for Energy and Performance Efficient Dynamic Consolidation of
 * Virtual Machines in Cloud Data Centers", Concurrency and Computation:
 * Practice and Experience (CCPE), Volume 24, Issue 13, Pages: 1397-1420, John
 * Wiley and Sons, Ltd, New York, USA, 2012</a>
 * </li>
 * </ul>
 * </p>
 *
 * @author Anton Beloglazov
 * @author Manoel Campos da Silva Filho
 * @since CloudSim Toolkit 3.0
 */
public abstract class VmAllocationPolicyMigrationAbstract extends VmAllocationPolicyAbstract implements VmAllocationPolicyMigration {
    public void setHostRamThreshold(boolean hostRamThreshold) {
        this.hostRamThreshold = hostRamThreshold;
    }

    public boolean isHostRamThreshold() {
        return hostRamThreshold;
    }

    private boolean hostRamThreshold = false;

    public boolean isEnableMigrateOneUnderLoadHost() {
        return enableMigrateOneUnderLoadHost;
    }

    public void setEnableMigrateOneUnderLoadHost(boolean enableMigrateOneUnderLoadHost) {
        this.enableMigrateOneUnderLoadHost = enableMigrateOneUnderLoadHost;
    }

    private boolean enableMigrateOneUnderLoadHost = false;
    public static final double DEF_UNDER_UTILIZATION_THRESHOLD = 0.35;
    public static final double DEF_RAM_UNDER_UTILIZATION_THRESHOLD = 0.35;
    /** @see #getUnderUtilizationThreshold() */
    private double underUtilizationThreshold;

    public double getUnderRamUtilizationThreshold() {
        return underRamUtilizationThreshold;
    }

    private double underRamUtilizationThreshold;

    /** @see #getVmSelectionPolicy() */
    private VmSelectionPolicy vmSelectionPolicy;

    /**
     * A map between a VM and the host where it is placed.
     */
    private final Map<Vm, Host> savedAllocation;

    /** @see #areHostsUnderloaded() */
    private boolean hostsUnderloaded;

    /** @see #areHostsOverloaded() */
    private boolean hostsOverloaded;


    /**
     * Creates a VmAllocationPolicy.
     * It uses a {@link #DEF_UNDER_UTILIZATION_THRESHOLD default under utilization threshold}.
     *
     * @param vmSelectionPolicy the policy that defines how VMs are selected for migration
     */
    public VmAllocationPolicyMigrationAbstract(final VmSelectionPolicy vmSelectionPolicy) {
        this(vmSelectionPolicy, null);
    }

    /**
     * Creates a new VmAllocationPolicy, changing the {@link Function} to select a Host for a Vm.
     * It uses a {@link #DEF_UNDER_UTILIZATION_THRESHOLD default under utilization threshold}.
     *
     * @param vmSelectionPolicy the policy that defines how VMs are selected for migration
     * @param findHostForVmFunction a {@link Function} to select a Host for a given Vm.
     *                              Passing null makes the Function to be set as the default {@link #findHostForVm(Vm)}.
     * @see VmAllocationPolicy#setFindHostForVmFunction(java.util.function.BiFunction)
     * @see #setUnderUtilizationThreshold(double)
     */
    public VmAllocationPolicyMigrationAbstract(
        final VmSelectionPolicy vmSelectionPolicy,
        final BiFunction<VmAllocationPolicy, Vm, Optional<Host>> findHostForVmFunction)
    {
        super(findHostForVmFunction);
        this.underUtilizationThreshold = DEF_UNDER_UTILIZATION_THRESHOLD;
        this.underRamUtilizationThreshold = DEF_RAM_UNDER_UTILIZATION_THRESHOLD;
        this.savedAllocation = new HashMap<>();
        setVmSelectionPolicy(vmSelectionPolicy);
    }

    @Override
    public Map<Vm, Host> getOptimizedAllocationMap(final List<? extends Vm> vmList) {
        //@TODO See https://github.com/manoelcampos/cloudsim-plus/issues/94
        final Set<Host> overloadedHosts = getOverloadedHosts();
        final Set<Host> switchedOffHosts = getSwitchedOffHosts();
        this.hostsOverloaded = !overloadedHosts.isEmpty();
        printOverUtilizedHosts(overloadedHosts);
        saveAllocation();
        //先找到过载的迁移表，再处理低负载的迁移表
        final Map<Vm, Host> migrationMap = getMigrationMapFromOverloadedHosts(overloadedHosts,switchedOffHosts);
        updateMigrationMapFromUnderloadedHosts(overloadedHosts, migrationMap,switchedOffHosts);
        if(migrationMap.isEmpty()){
            return migrationMap;
        }

        restoreAllocation();
        return migrationMap;
    }

    /**
     * Updates the  map of VMs that will be migrated from under utilized hosts.
     *
     * @param overloadedHosts the List of over utilized hosts
     * @param migrationMap current migration map that will be updated
     */
    protected void updateMigrationMapFromUnderloadedHosts(
        final Set<Host> overloadedHosts,
        final Map<Vm, Host> migrationMap,
        final Set<Host> switchedOffHosts)
    {

        // overloaded hosts + hosts that are selected to migrate VMs from overloaded hosts
        final Set<Host> ignoredSourceHosts = getIgnoredHosts(overloadedHosts, switchedOffHosts);

        /*
        During the computation of the new placement for VMs,
        the current VM placement is changed temporarily, before the actual migration of VMs.
        If VMs are being migrated from overloaded Hosts, they in fact already were removed
        from such Hosts and moved to destination ones.
        The target Host that maybe was shut down, might become underloaded too.
        This way, such Hosts are added to be ignored when
        looking for underloaded Hosts.
        See https://github.com/manoelcampos/cloudsim-plus/issues/94
         */
        //过载被迁移的主机有可能是新开启的，因此是低负载的
        //这个是低负载前迁出的忽略列表，不是迁入的，若有因为过载被迁入的host不再参与迁出
        ignoredSourceHosts.addAll(migrationMap.values());

        // overloaded + underloaded hosts
        final Set<Host> ignoredTargetHosts = getIgnoredHosts(overloadedHosts, switchedOffHosts);

        final int numberOfHosts = getHostList().size();

        this.hostsUnderloaded = false;
        if(isEnableMigrateOneUnderLoadHost()){
            final Host underloadedHost = getUnderloadedHost(ignoredSourceHosts);
            if (underloadedHost == Host.NULL) {
                return;
            }
            this.hostsUnderloaded = true;

            //或许可以不打印，太多了
//            printUnderUtilizedHosts(underloadedHost);

            LOGGER.info("{}: VmAllocationPolicy: Underloaded hosts: {}", getDatacenter().getSimulation().clockStr(), underloadedHost);

            ignoredSourceHosts.add(underloadedHost);
            ignoredTargetHosts.add(underloadedHost);

            final List<? extends Vm> vmsToMigrateFromHost = getVmsToMigrateFromUnderUtilizedHost(underloadedHost);
            if (!vmsToMigrateFromHost.isEmpty()) {
                logVmsToBeReallocated(underloadedHost, vmsToMigrateFromHost);
                final Map<Vm, Host> newVmPlacement = getNewVmPlacementFromUnderloadedHost(
                    vmsToMigrateFromHost,
                    ignoredTargetHosts,
                    underloadedHost);

                ignoredSourceHosts.addAll(extractHostListFromMigrationMap(newVmPlacement));
                migrationMap.putAll(newVmPlacement);
            }
        }else{
            while (true) {
                //当所有低于值host都被遍历，ignoredhost的数目等于系统中host的数目
                if (numberOfHosts == ignoredSourceHosts.size()) {
                    break;
                }
//                for(Host host:getHostList()){
//                    if(host.getId() == 250){
//                        System.out.println("大小："+host.getVmList().size());
//                        for(Vm vm:host.getVmList()){
//                            System.out.println("here:"+vm+"  "+vm.getCurrentRequestedMips()+"  "+host.getVmScheduler().getAllocatedMips(vm)+ " "+vm.getCpuUtilizationBeforeMigration());
//                        }
//                    }
//                }
                //每次循环选择一个低负载的host出来
                final Host underloadedHost = getUnderloadedHost(ignoredSourceHosts);
                if (underloadedHost == Host.NULL) {
                    break;
                }
                this.hostsUnderloaded = true;

                //或许可以不打印，太多了
            printUnderUtilizedHosts(underloadedHost);

                LOGGER.info("{}: VmAllocationPolicy: Underloaded hosts: {}", getDatacenter().getSimulation().clockStr(), underloadedHost);

                ignoredSourceHosts.add(underloadedHost);
                ignoredTargetHosts.add(underloadedHost);

                final List<? extends Vm> vmsToMigrateFromHost = getVmsToMigrateFromUnderUtilizedHost(underloadedHost);
                if (!vmsToMigrateFromHost.isEmpty()) {
                    logVmsToBeReallocated(underloadedHost, vmsToMigrateFromHost);
                    final Map<Vm, Host> newVmPlacement = getNewVmPlacementFromUnderloadedHost(
                        vmsToMigrateFromHost,
                        ignoredTargetHosts,
                        underloadedHost);

                    ignoredSourceHosts.addAll(extractHostListFromMigrationMap(newVmPlacement));
                    migrationMap.putAll(newVmPlacement);
                }
            }
        }
    }

    private void logVmsToBeReallocated(final Host underloadedHost, final List<? extends Vm> migratingOutVms) {
        if(LOGGER.isInfoEnabled()) {
            LOGGER.info("{}: VmAllocationPolicy: VMs to be reallocated from the underloaded {}: {}",
                getDatacenter().getSimulation().clockStr(), underloadedHost, getVmIds(migratingOutVms));
        }
    }

    private Set<Host> getIgnoredHosts(final Set<Host> overloadedHosts, final Set<Host> switchedOffHosts) {
        final Set<Host> ignoredHosts = new HashSet<>();
        ignoredHosts.addAll(overloadedHosts);
        ignoredHosts.addAll(switchedOffHosts);
        return ignoredHosts;
    }

    private String getVmIds(final List<? extends Vm> vmList) {
        return vmList.stream().map(vm -> String.valueOf(vm.getId())).collect(Collectors.joining(", "));
    }

    /**
     * Prints the over utilized hosts.
     *
     * @param overloadedHosts the over utilized hosts
     */
    private void printOverUtilizedHosts(final Set<Host> overloadedHosts) {
        if (!overloadedHosts.isEmpty() && LOGGER.isWarnEnabled()) {
            final String hosts = overloadedHosts.stream().map(this::overloadedHostToString).collect(Collectors.joining(System.lineSeparator()));
            LOGGER.warn("{}: VmAllocationPolicy: Overloaded hosts in {}:{}{}",
                getDatacenter().getSimulation().clockStr(), getDatacenter(), System.lineSeparator(), hosts);
        }
    }

    private void printUnderUtilizedHosts(final Host host) {
        if (host != null && LOGGER.isWarnEnabled()) {
            final String hostimfor = underloadedHostToString(host);
            LOGGER.warn("{}: VmAllocationPolicy: Underloaded hosts in {}:{}{}",
                getDatacenter().getSimulation().clockStr(), getDatacenter(), System.lineSeparator(), hostimfor);
        }
//        for(Vm vm:host.getVmList()){
//            System.out.println(vm+"  "+vm.getCurrentRequestedMips()+"  "+host.getVmScheduler().getAllocatedMips(vm)+ " "+vm.getCpuUtilizationBeforeMigration()+" "+host.getRam().getAvailableResource());
//        }
    }

    private String overloadedHostToString(final Host host) {
        return String.format(
            "      Host %d (upper CPU threshold %.2f, current CPU utilization: %.2f,upper RAM threshold %.2f, current RAM utilization: %.2f)",
            host.getId(), getOverUtilizationThreshold(host), host.getCpuPercentUtilization(),getRamOverUtilizationThreshold(host),host.getRamPercentUtilization());
    }

    private String underloadedHostToString(final Host host) {
        return String.format(
            "      Host %d (lower CPU threshold %.2f, current CPU utilization: %.2f,lower RAM threshold %.2f, current RAM utilization: %.2f) ,current holding vm size: %d",
            host.getId(), getUnderUtilizationThreshold(), host.getCpuPercentUtilization(),getUnderRamUtilizationThreshold(),host.getRamPercentUtilization(),host.getVmList().size());
    }


    /**
     * Gets the power consumption different after the supposed placement of a VM into a given Host
     * and the original Host power consumption.
     *
     * @param host the host to check the power consumption
     * @param vm the candidate vm
     * @return the host power consumption different after the supposed VM placement or 0 if the power
     * consumption could not be determined
     */
    protected double getPowerDifferenceAfterAllocation(final Host host, final Vm vm){
        final double powerAfterAllocation = getPowerAfterAllocation(host, vm);
        if (powerAfterAllocation > 0) {
            return powerAfterAllocation - host.getPowerModel().getPower();
        }

        return 0;
    }

    /**
     * Checks if a host will be over utilized after placing of a candidate VM.
     *
     * @param host the host to verify
     * @param vm the candidate vm
     * @return true, if the host will be over utilized after VM placement;
     *         false otherwise
     */
    protected boolean isNotHostOverloadedAfterAllocation(final Host host, final Vm vm) {
//        System.out.println("before here:"+vm+"  "+vm.getCurrentRequestedMips()+"  "+host.getVmScheduler().getAllocatedMips(vm)+ " "+vm.getCpuUtilizationBeforeMigration());
        final Vm tempVm = new VmSimple(vm,true);
//        System.out.println("tempvm的ram："+tempVm.getCurrentRequestedRam());
//        System.out.println(host.getSimulation().clockStr()+" : after: "+vm+" 当前的ram利用： "+vm.getCurrentRequestedRam());
//        tempVm.setRam(vm.getCurrentRequestedRam());
//        tempVm.setBw(vm.getCurrentRequestedBw());
        HostSuitability suitability = host.createTemporaryVm(tempVm);
        if (!suitability.fully()) {
//            if(!suitability.forRam() && host.getSimulation().clock() <= 10801.0 && host.getSimulation().clock() >= 10800.0){
//                System.out.println(host+" "+vm+" "+" host可用ram是"+host.getRam().getAvailableResource()+"  vm需要的ram是："+vm.getCurrentRequestedRam()+ "  tempVm需要的是"+tempVm.getRam().getCapacity()+ " currentAllocatedResource: "+host.getRamProvisioner().getAllocatedResourceForVm(vm));
//            }
            System.out.println(vm+" 过滤剩下的"+host+"本应该可以放进去，但是实际因为容量不足放不进去");
            return false;
        }

        final double usagePercent = host.getCpuPercentUtilization();
        final boolean notOverloadedAfterAllocation;
        if(isHostRamThreshold()){
            final double usageRamPercent = getHostRamPercentRequested(host);
            notOverloadedAfterAllocation = !isHostOverloaded(host, usagePercent,usageRamPercent);
        }else{
            notOverloadedAfterAllocation = !isHostOverloaded(host, usagePercent);
        }
        //这里出现了问题
//        if(host.getId() == 250 && host.getSimulation().clock() > 0.2){
//            Vm nvm = host.getVmList().get(0);
//            System.out.println("before: "+host.getVmScheduler().getAllocatedMips(nvm) +" ram:"+ host.getRam().getAvailableResource()+" mips:"+host.getVmScheduler().getTotalAvailableMips());
//        }
        host.destroyTemporaryVm(tempVm);
//        if(host.getId() == 250 && host.getSimulation().clock() > 0.2){
//            Vm nvm = host.getVmList().get(0);
//            System.out.println("after: "+host.getVmScheduler().getAllocatedMips(nvm) +" ram:"+ host.getRam().getAvailableResource()+" mips:"+host.getVmScheduler().getTotalAvailableMips());
//        }
        return notOverloadedAfterAllocation;
    }

    /**
     * {@inheritDoc}
     * It's based on current CPU usage.
     *
     * @param host {@inheritDoc}
     * @return {@inheritDoc}
     */
    @Override
    public boolean isHostOverloaded(final Host host) {
        if(hostRamThreshold){
            final double hostCpuUtilization = host.getCpuPercentUtilization();
            final double hostRamUtilization = host.getRamPercentUtilization();

//            if(hostCpuUtilization >= 1.0 || hostRamUtilization >= 1.0) host.setTotalOver100Time(host.getTotalOver100Time() + 1);
//            System.out.println("执行了判断");
            return isHostOverloaded(host, hostCpuUtilization,hostRamUtilization);
        }else{
            return isHostOverloaded(host, host.getCpuPercentUtilization());
        }
    }

    /**
     * Checks if a Host is overloaded based on the given CPU utilization percent.
     * @param host the Host to check
     * @param cpuUsagePercent the Host's CPU utilization percent. The values may be:
     *                        <ul>
     *                          <li>the current CPU utilization if you want to check if the Host is overloaded right now;</li>
     *                          <li>the requested CPU utilization after temporarily placing a VM into the Host
     *                          just to check if it supports that VM without being overloaded.
     *                          In this case, if the Host doesn't support the already placed temporary VM,
     *                          the method will return true to indicate the Host will be overloaded
     *                          if the VM is actually placed into it.
     *                          </li>
     *                        </ul>
     * @return true if the Host is overloaded, false otherwise
     */
    protected boolean isHostOverloaded(final Host host, final double cpuUsagePercent){
//        if(cpuUsagePercent > getOverUtilizationThreshold(host)){
//            System.out.printf(
//                "      Host %d (upper CPU threshold %.2f, current CPU utilization: %.2f,upper RAM threshold %.2f, current RAM utilization: %.2f)%n",
//                host.getId(), getOverUtilizationThreshold(host), cpuUsagePercent,getRamOverUtilizationThreshold(host),host.getRamPercentUtilization());
//        }
        return cpuUsagePercent > getOverUtilizationThreshold(host);
    }

    protected boolean isHostOverloaded(final Host host, final double cpuUsagePercent, final double ramUsagePercent){
        return cpuUsagePercent > getOverUtilizationThreshold(host) && ramUsagePercent > getRamOverUtilizationThreshold(host);
    }

    protected boolean isHostOverloadedAfter(final Host host, final double cpuUsagePercent, final double ramUsagePercent){
        return cpuUsagePercent > getOverUtilizationThreshold(host) || ramUsagePercent > getRamOverUtilizationThreshold(host);
    }

    /**
     * Checks if a host is under utilized, based on current CPU usage.
     *
     * @param host the host
     * @return true, if the host is under utilized; false otherwise
     */
    @Override
    public boolean isHostUnderloaded(final Host host) {
        if(hostRamThreshold){
            return isHostUnderloaded(host.getCpuPercentUtilization(),host.getRamPercentUtilization());
        }else{
            return isHostUnderloaded(host.getCpuPercentUtilization());
        }
//        return getHostCpuPercentRequested(host) < getUnderUtilizationThreshold();
    }

    public boolean isHostUnderloaded(final double cpuUsagePercent,final double ramUsagePercent) {
        return cpuUsagePercent < getUnderUtilizationThreshold() && ramUsagePercent < getUnderRamUtilizationThreshold();
    }

    public boolean isHostUnderloaded(final double cpuUsagePercent) {
        return cpuUsagePercent < getUnderUtilizationThreshold();
    }


    @Override
    protected Optional<Host> defaultFindHostForVm(final Vm vm) {
        final Set<Host> excludedHosts = new HashSet<>();
        excludedHosts.add(vm.getHost());
        return findHostForVm(vm, excludedHosts);
    }

    /**
     * Finds a Host that has enough resources to place a given VM and that will not
     * be overloaded after the placement. The selected Host will be that
     * one with most efficient power usage for the given VM.
     *
     * <p>This method performs the basic filtering and delegates additional ones
     * and the final selection of the Host to other method.</p>
     *
     * @param vm the VM
     * @param excludedHosts the excluded hosts
     * @return an {@link Optional} containing a suitable Host to place the VM or an empty {@link Optional} if not found
     * @see #findHostForVmInternal(Vm, Stream)
     */
    private Optional<Host> findHostForVm(final Vm vm, final Set<? extends Host> excludedHosts) {
        vm.setSearchForHost(true);
        /*The predicate always returns true to indicate that, in fact, it is not
        applying any additional filter.*/
        return findHostForVm(vm, excludedHosts, host -> true);
    }

    /**
     * Finds a Host that has enough resources to place a given VM and that will not
     * be overloaded after the placement. The selected Host will be that
     * one with most efficient power usage for the given VM.
     *
     * <p>This method performs the basic filtering and delegates additional ones
     * and the final selection of the Host to other method.</p>
     *
     * @param vm the VM
     * @param excludedHosts the excluded hosts
     * @param predicate an additional {@link Predicate} to be used to filter
     *                  the Host to place the VM
     * @return an {@link Optional} containing a suitable Host to place the VM or an empty {@link Optional} if not found
     * @see #findHostForVmInternal(Vm, Stream)
     */
    private Optional<Host> findHostForVm(final Vm vm, final Set<? extends Host> excludedHosts, final Predicate<Host> predicate) {
        final Stream<Host> stream = this.getHostList().stream()
            .filter(host -> !excludedHosts.contains(host))
            .filter(Host::isActive)
            .filter(host -> host.isSuitableForVm(vm))
            .filter(host -> isNotHostOverloadedAfterAllocation(host, vm))
            .filter(predicate);

        return findHostForVmInternal(vm, stream);
    }

    /**
     * Applies additional filters to the Hosts Stream and performs the actual Host selection.
     * This method is a Stream's final operation, that it, it closes the Stream and returns an {@link Optional} value.
     *
     * <p>This method can be overridden by sub-classes to change the method used to select the Host for the given VM.</p>
     *
     * @param vm the VM to find a Host to be placed into
     * @param hostStream a {@link Stream} containing the Hosts after passing the basic filtering
     * @return an {@link Optional} containing a suitable Host to place the VM or an empty {@link Optional} if not found
     * @see #findHostForVm(Vm, Set)
     */
    protected Optional<Host> findHostForVmInternal(final Vm vm, final Stream<Host> hostStream){
        final Comparator<Host> hostPowerConsumptionComparator =
            comparingDouble(host -> getPowerDifferenceAfterAllocation(host, vm));
        return hostStream.min(hostPowerConsumptionComparator);
    }

    /**
     * Extracts the host list from a migration map.
     *
     * @param migrationMap the migration map
     * @return the list
     */
    private List<Host> extractHostListFromMigrationMap(final Map<Vm, Host> migrationMap) {
        return new ArrayList<>(migrationMap.values());
    }

    /**
     * Gets a new VM placement considering the list of VM to migrate
     * from overloaded Hosts.
     *
     * @param overloadedHosts the list of overloaded Hosts
     * @return the new VM placement map where each key is a VM
     * and each value is the Host to place it.
     * @TODO See issue in {@link #getVmsToMigrateFromOverloadedHost(Host)}
     */
    private Map<Vm, Host> getMigrationMapFromOverloadedHosts(final Set<Host> overloadedHosts,Set<Host> switchedOffHosts) {
        if(overloadedHosts.isEmpty()) {
            return  new HashMap<>();
        }

        //所有过载主机中拿出来的host都放在这里
        final List<Vm> vmsToMigrate = getVmsToMigrateFromOverloadedHosts(overloadedHosts);
        //(更改)这里sort了个寂寞，没有用的
        sortByCpuUtilization(vmsToMigrate, getDatacenter().getSimulation().clock());
        final Map<Vm, Host> migrationMap = new HashMap<>();

        final StringBuilder builder = new StringBuilder();
        for (final Vm vm : vmsToMigrate) {
            Optional<Host> targethost = findHostForVm(vm, overloadedHosts);
            if(targethost.isPresent()){
                Host host = targethost.get();
                addVmToMigrationMap(migrationMap, vm, host);
                appendVmMigrationMsgToStringBuilder(builder, vm, host);
            }else{
                System.out.println(getDatacenter().getSimulation().clockStr() + ": Vm "+ vm.getId()+" in source " + vm.getHost() +" can't find a host to place.now trying to awake a sleep host!");
                //如果有vm没有找到host，没有操作，这里需要开启一台host来进行放置
                final Stream<Host> stream = switchedOffHosts.stream()
                    .filter(host -> host.isSuitableForVm(vm))
                    .filter(host -> isNotHostOverloadedAfterAllocation(host, vm));
                Optional<Host> target = findHostForVmInternal(vm,stream);
                Host host = null;
                if(target.isPresent()){
                    host = target.get();
                    host.setActive(true);
                    addVmToMigrationMap(migrationMap, vm, host);
                    appendVmMigrationMsgToStringBuilder(builder, vm, host);
                    System.out.println(getDatacenter().getSimulation().clockStr() + ": Host "+ host.getId()+" has been awake for Vm " +vm.getId()+" migration successful!");
                    System.out.println();
                }else{
                    System.out.println(getDatacenter().getSimulation().clockStr() + ": 没有找到合适的host给 "+ vm + " 进行过载迁移");
                    System.out.println();
                }
//                for(Host host:switchedOffHosts){
//                    if(host.isSuitableForVm(vm)){
//                        host.setActive(true);
//                        addVmToMigrationMap(migrationMap, vm, host);
//                        appendVmMigrationMsgToStringBuilder(builder, vm, host);
//                        target = host;
//                        System.out.println(getDatacenter().getSimulation().clockStr() + ": Host "+ host.getId()+" has been awake for Vm " +vm.getId()+" migration successful!");
//                        break;
//                    }
//                }
                switchedOffHosts.remove(host);

            }
//            findHostForVm(vm, overloadedHosts).ifPresent(targetHost -> {
//                addVmToMigrationMap(migrationMap, vm, targetHost);
//                appendVmMigrationMsgToStringBuilder(builder, vm, targetHost);
//            });
        }
        LOGGER.info(
            "{}: VmAllocationPolicy: Reallocation of VMs from overloaded hosts: {}{}",
            getDatacenter().getSimulation().clockStr(), System.lineSeparator(), builder.toString());

        return migrationMap;
    }

    private void appendVmMigrationMsgToStringBuilder(final StringBuilder builder, final Vm vm, final Host targetHost) {
        if(LOGGER.isInfoEnabled()) {
            builder.append("      ").append(vm).append(" will be migrated from ")
              .append(vm.getHost()).append(" to ").append(targetHost)
              .append(System.lineSeparator());
        }
    }

    /**
     * Gets a new placement for VMs from an underloaded host.
     *
     * @param vmsToMigrate the list of VMs to migrate from the underloaded Host
     * @param excludedHosts the list of hosts that aren't selected as
     * destination hosts
     * @return the new vm placement for the given VMs
     */
    private Map<Vm, Host> getNewVmPlacementFromUnderloadedHost(
        final List<? extends Vm> vmsToMigrate,
        final Set<? extends Host> excludedHosts,
        Host underloadedHost)
    {
        final Map<Vm, Host> migrationMap = new HashMap<>();
        //低负载vm和高负载vm是分开迁移的，但是都是按bfd进行迁移
        sortByCpuUtilization(vmsToMigrate, getDatacenter().getSimulation().clock());
        for (final Vm vm : vmsToMigrate) {

            //如果这个vm的cloudlet已经完成了，但是还没有销毁，禁止迁移一个空虚拟机，会自己销毁
            if (vm.getCloudletScheduler().isEmpty()) {
                return new HashMap<>();
            }

            //try to find a target Host to place a VM from an underloaded Host that is not underloaded too
            //（更改）万一没有低负载迁移，问题出在这里
            final Optional<Host> optional = findHostForVm(vm, excludedHosts, host -> true);
            //只要有一个vm找不到host，直接返回空map，之前找到host的也不算了
            if (!optional.isPresent()) {
                for(Map.Entry<Vm,Host> entry:migrationMap.entrySet()){
                    entry.getValue().destroyTemporaryVm(entry.getKey());
                }
                System.out.println("无法放置所有低负载vms");
                //不打印
//                LOGGER.warn(
//                    "{}: VmAllocationPolicy: A new Host, which isn't also underloaded or won't be overloaded, couldn't be found to migrate {}. Migration of VMs from the underloaded {} cancelled.",
//                    getDatacenter().getSimulation().clockStr(), vm, vm.getHost());
                return new HashMap<>();
            }
            addVmToMigrationMap(migrationMap, vm, optional.get());
        }
        System.out.println(getDatacenter().getSimulation().clockStr()+" host "+underloadedHost.getId()+" 因为低负载，vms全部被迁移出去，因此闲置关闭，过段时间系统自动关闭" );
//        underloadedHost.setActive(false);
        return migrationMap;
    }

    /**
     * Sort a given list of VMs by descending order of CPU utilization.
     *
     * @param vmList the vm list to be sorted
     * @param simulationTime the simulation time to get the current CPU utilization for each Vm
     */
    protected void sortByCpuUtilization(final List<? extends Vm> vmList, final double simulationTime) {
        final Comparator<Vm> comparator = comparingDouble(vm -> vm.getTotalMipsCapacity() * vm.getCpuUtilizationBeforeMigration());
        vmList.sort(comparator.reversed());
    }

    private <T extends Host> void addVmToMigrationMap(final Map<Vm, T> migrationMap, final Vm vm, final T targetHost) {
        /*
        Temporarily creates the VM into the target Host so that
        when the next VM is got to be migrated, if the same Host
        is selected as destination, the resource to be
        used by the previous VM will be considered when
        assessing the suitability of such a Host for the next VM.
         */
        targetHost.createTemporaryVm(vm);
        migrationMap.put(vm, targetHost);
    }

    /**
     * Gets the VMs to migrate from Hosts.
     *
     * @param overloadedHosts the List of overloaded Hosts
     * @return the VMs to migrate from hosts
     */
    private List<Vm> getVmsToMigrateFromOverloadedHosts(final Set<Host> overloadedHosts) {
        final List<Vm> vmsToMigrate = new LinkedList<>();
        for (final Host host : overloadedHosts) {
            vmsToMigrate.addAll(getVmsToMigrateFromOverloadedHost(host));
        }

        return vmsToMigrate;
    }
    private List<Vm> getVmsToMigrateFromOverloadedHost(final Host host) {
        /*
        @TODO The method doesn't just gets a list of VMs to migrate from an overloaded Host,
        but it temporarily destroys VMs on such Hosts.
        See https://github.com/manoelcampos/cloudsim-plus/issues/94
        */
        final List<Vm> vmsToMigrate = new LinkedList<>();
        while (true) {
            final Vm vm = getVmSelectionPolicy().getVmToMigrate(host);
            if (Vm.NULL == vm || vm.getCloudletScheduler().isEmpty()) {
                break;
            }
            vmsToMigrate.add(vm);
            /*Temporarily destroys the selected VM into the overloaded Host so that
            the loop gets VMs from such a Host until it is not overloaded anymore.*/
            host.destroyTemporaryVm(vm);
            if (!isHostOverloaded(host)) {
                break;
            }
        }

        return vmsToMigrate;
    }

    /**
     * Gets the VMs to migrate from under utilized host.
     *
     * @param host the host
     * @return the vms to migrate from under utilized host
     */
    //（要改），不知道会不会把vm变成iscreate
    protected List<? extends Vm> getVmsToMigrateFromUnderUtilizedHost(final Host host) {
        return host.getMigratableVms();
    }

    /**
     * Gets the switched off hosts.
     *
     * @return the switched off hosts
     */
    protected Set<Host> getSwitchedOffHosts() {
        return this.getHostList().stream()
            .filter(this::isShutdownOrFailed)
            .collect(toSet());
    }

    private boolean isShutdownOrFailed(final Host host) {
        return !host.isActive() || host.isFailed();
    }

    /**
     * Gets the List of overloaded hosts.
     * If a Host is overloaded but it has VMs migrating out,
     * then it's not included in the returned List
     * because the VMs to be migrated to move the Host from
     * the overload state already are in migration.
     *
     * @return the over utilized hosts
     */
    private Set<Host> getOverloadedHosts() {
        return this.getHostList().stream()
            .filter(Host::isActive)
            .filter(this::isHostOverloaded)
            .filter(host -> host.getVmsMigratingOut().isEmpty())
            .collect(toSet());
    }

    /**
     * Gets the most underloaded Host.
     * If a Host is underloaded but it has VMs migrating in,
     * then it's not included in the returned List
     * because the VMs to be migrated to move the Host from
     * the underload state already are in migration to it.
     * Likewise, if all VMs are migrating out, nothing has to be
     * done anymore. It just has to wait the VMs to finish
     * the migration.
     *
     * @param excludedHosts the Hosts that have to be ignored when looking for the under utilized Host
     * @return the most under utilized host or {@link Host#NULL} if no Host is found
     */
    protected Host getUnderloadedHost(final Set<? extends Host> excludedHosts) {
        return this.getHostList().stream()
            .filter(host -> !excludedHosts.contains(host))
            .filter(Host::isActive)
            .filter(this::isHostUnderloaded)
            .filter(host -> host.getVmsMigratingIn().isEmpty())
            .filter(this::notAllVmsAreMigratingOut)
            .min(comparingDouble(Host::getCpuPercentUtilization))
            .orElse(Host.NULL);
    }

    protected double getHostCpuPercentRequested(final Host host) {
        return getHostTotalRequestedMips(host) / host.getTotalMipsCapacity();
    }

    protected double getHostRamPercentRequested(final Host host) {
        return getHostTotalRequestedRam(host) / host.getRam().getCapacity();
    }

    protected double getFutureHostCpuPercentRequested(final Host host,final double cpuUsage) {
        return cpuUsage / host.getTotalMipsCapacity();
    }

    /**
     * Gets the total MIPS that is currently being used by all VMs inside the Host.
     * @param host
     * @return
     */
    private double getHostTotalRequestedMips(final Host host) {
        return host.getVmList().stream()
            .mapToDouble(Vm::getCurrentRequestedTotalMips)
            .sum();
    }

    private double getHostTotalRequestedRam(final Host host) {
        return host.getVmList().stream()
            .mapToDouble(Vm::getCurrentRequestedRam)
            .sum();
    }

    /**
     * Checks if all VMs of a Host are <b>NOT</b> migrating out.
     * In this case, the given Host will not be selected as an underloaded Host at the current moment.
     * That is: not all VMs are migrating out if at least one VM isn't in migration process.
     *
     * @param host the host to check
     * @return true if at least one VM isn't migrating, false if all VMs are migrating
     */
    protected boolean notAllVmsAreMigratingOut(final Host host) {
        return host.getVmList().stream().anyMatch(vm -> !vm.isInMigration());
    }

    /**
     * Saves the current map between a VM and the host where it is placed.
     *
     * @see #savedAllocation
     */
    private void saveAllocation() {
        savedAllocation.clear();
        for (final Host host : getHostList()) {
            final VmScheduler vmScheduler = host.getVmScheduler();

            //更改，正在中的不能分配？
            //抹除所有host上所有ram分配
            ResourceProvisioner ramProvisioner = host.getRamProvisioner();
            ramProvisioner.deallocateResourceForAllVms();
            List<Vm> removeDestroyVms = new ArrayList<>();
            for (final Vm vm : host.getVmList()) {

                //排除掉已完成改毁灭的虚拟机
                if(vm.isDestory() && vm.getCloudletScheduler().isEmpty()){
                    removeDestroyVms.add(vm);
                    continue;
                }

                //(修改更新的host的ram provisioner),有可能会溢出，要修改
                ramProvisioner.allocateResourceForVm(vm, vm.getCurrentRequestedRam());

//                System.out.println("saveAllocation:"+vm+"  "+vm.getCurrentRequestedMips()+"  "+host.getVmScheduler().getAllocatedMips(vm)+ " "+vm.getCpuUtilizationBeforeMigration());

                /* TODO: this VM loop has a quadratic wost-case complexity (when
                    all Vms already in the VM list are migrating into this Host).
                *  Instead of looping over the vmsMigratingIn list for every VM,
                *  we could add a Host migratingIn attribute to the Vm.
                * Then, for every VM on the Host, we check this VM attribute
                * to see if the VM is migrating into the Host. */
                if (!host.getVmsMigratingIn().contains(vm)) {
                    savedAllocation.put(vm, host);
                    //记录下每个vm当前的cpu利用率
                    vm.setCpuUtilizationBeforeMigration(vm.getCpuPercentUtilization());

                    //（更改）修改每个更新后的vm已分配的mips，有可能会溢出，导致available为负数，在恢复的时候需要forceplace
                    MipsShare mipsShare = vm.getCurrentUtilizationMips();
                    vmScheduler.getAllocatedMipsMap().put(vm,new MipsShare(mipsShare.pes(), mipsShare.mips()*vmScheduler.percentOfMipsToRequest(vm)));
                }else{
                    System.out.println("执行了！！！"+vm);
                }
            }
            for(final Vm vm:host.getVmsMigratingIn()){
                //(修改更新的host的ram provisioner),有可能会溢出，要修改
                ramProvisioner.allocateResourceForVm(vm, vm.getCurrentRequestedRam());
            }
            for(Vm vm:removeDestroyVms){
                vm.setCreated(true);
                host.destroyVm(vm);
            }
//            System.out.println(host+ " vmsize:"+host.getVmList().size()+" mips:"+host.getVmScheduler().getTotalAvailableMips()+" ram:"+host.getRam().getAvailableResource());
        }
    }

    /**
     * Restore VM allocation from the allocation history.
     *  TODO: The allocation map only needs to be restored because
     *  VMs are destroyed in order to assess a new VM placement.
     *  After fixing this issue, there will be no need to restore VM mapping.
     *  https://github.com/manoelcampos/cloudsim-plus/issues/94
     *
     * @see #savedAllocation
     */
    private void restoreAllocation() {
        for (final Host host : getHostList()) {
            host.destroyAllVms();
            host.reallocateMigratingInVms();
        }

        for (final Vm vm : savedAllocation.keySet()) {
            final Host host = savedAllocation.get(vm);
            HostSuitability hostSuitability = host.createTemporaryVm(vm);
            if (hostSuitability.fully()){
                vm.setCreated(true);
            }
            //有可能放不回去，因为原本利用率太高
            else{
                LOGGER.error("VmAllocationPolicy: Couldn't restore {} on {}, now try to force it to restore", vm, host);
                if(!hostSuitability.forRam()){
                    vm.setForcePlace(true);
                }
                if(!hostSuitability.forPes()){

                }
                if(host.createTemporaryVm(vm).fully()){
                    vm.setCreated(true);
                    LOGGER.error("VmAllocationPolicy:force {} to restore on {} successsful!",vm,host);
                }else{
                    LOGGER.error("VmAllocationPolicy: force restore {} on {} failed!", vm, host);
                }

                vm.setForcePlace(false);
                System.out.println();
            };
        }
    }

    /**
     * Gets the power consumption of a host after the supposed placement of a candidate VM.
     * The VM is not in fact placed at the host.
     *
     * @param host the host to check the power consumption
     * @param vm the candidate vm
     *
     * @return the host power consumption after the supposed VM placement or 0 if the power
     * consumption could not be determined
     */
    protected double getPowerAfterAllocation(final Host host, final Vm vm) {
        try {
            return host.getPowerModel().getPower(getMaxUtilizationAfterAllocation(host, vm));
        } catch (IllegalArgumentException e) {
            LOGGER.error("Power consumption for {} could not be determined: {}", host, e.getMessage());
        }

        return 0;
    }

    /**
     * Gets the max power consumption of a host after placement of a candidate
     * VM. The VM is not in fact placed at the host. We assume that load is
     * balanced between PEs. The only restriction is: VM's max MIPS less than PE's MIPS
     *
     * @param host the host
     * @param vm the vm
     * @return the power after allocation
     */
    protected double getMaxUtilizationAfterAllocation(final Host host, final Vm vm) {
        final double requestedTotalMips = vm.getCurrentRequestedTotalMips();
        final double hostUtilizationMips = getUtilizationOfCpuMips(host);
        final double hostPotentialMipsUse = hostUtilizationMips + requestedTotalMips;
        return hostPotentialMipsUse / host.getTotalMipsCapacity();
    }

    /**
     * Gets the utilization of the CPU in MIPS for the current potentially
     * allocated VMs.
     *
     * @param host the host
     *
     * @return the utilization of the CPU in MIPS
     */
    protected double getUtilizationOfCpuMips(final Host host) {
        double hostUtilizationMips = 0;
        for (final Vm vm : host.getVmList()) {
            final double additionalMips = additionalCpuUtilizationDuringMigration(host, vm);
            hostUtilizationMips += additionalMips + host.getTotalAllocatedMipsForVm(vm);
        }

        return hostUtilizationMips;
    }

    /**
     * Calculate additional potential CPU usage of a VM migrating into a given Host.
     * @param host the Hosts that is being computed the current utilization of CPU MIPS
     * @param vm a VM from that Host
     * @return the additional amount of MIPS the Host will use if the VM is migrating into it, 0 otherwise
     */
    private double additionalCpuUtilizationDuringMigration(final Host host, final Vm vm) {
        if (!host.getVmsMigratingIn().contains(vm)) {
            return 0;
        }

        final double maxCpuUtilization = host.getVmScheduler().getMaxCpuUsagePercentDuringOutMigration();
        final double migrationOverhead = host.getVmScheduler().getVmMigrationCpuOverhead();
        return host.getTotalAllocatedMipsForVm(vm) * maxCpuUtilization / migrationOverhead;
    }

    @Override
    public final void setVmSelectionPolicy(final VmSelectionPolicy vmSelectionPolicy) {
        this.vmSelectionPolicy = Objects.requireNonNull(vmSelectionPolicy);
    }

    @Override
    public VmSelectionPolicy getVmSelectionPolicy() {
        return vmSelectionPolicy;
    }

    @Override
    public double getUnderUtilizationThreshold() {
        return underUtilizationThreshold;
    }

    @Override
    public void setUnderUtilizationThreshold(final double underUtilizationThreshold) {
        if(underUtilizationThreshold <= 0 || underUtilizationThreshold >= 1){
            throw new IllegalArgumentException("Under utilization threshold must be greater than 0 and lower than 1.");
        }

        this.underUtilizationThreshold = underUtilizationThreshold;
    }

    public void setUnderUtilizationThreshold(final double underUtilizationThreshold,final double underRamUtilizationThreshold) {
        if(underUtilizationThreshold <= 0 || underUtilizationThreshold >= 1){
            throw new IllegalArgumentException("Under Cpu utilization threshold must be greater than 0 and lower than 1.");
        }
        if(underRamUtilizationThreshold <= 0 || underRamUtilizationThreshold >= 1){
            throw new IllegalArgumentException("Under Ram utilization threshold must be greater than 0 and lower than 1.");
        }

        this.underUtilizationThreshold = underUtilizationThreshold;
        this.underRamUtilizationThreshold = underRamUtilizationThreshold;
    }

    @Override
    public final boolean isVmMigrationSupported() {
        return true;
    }

    @Override
    public boolean areHostsUnderloaded() {
        return hostsUnderloaded;
    }

    @Override
    public boolean areHostsOverloaded() {
        return hostsOverloaded;
    }
}
