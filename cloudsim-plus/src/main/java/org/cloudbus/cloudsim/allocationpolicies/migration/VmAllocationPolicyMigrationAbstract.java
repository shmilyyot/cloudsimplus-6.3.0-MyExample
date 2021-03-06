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
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Comparator.comparingDouble;
import static java.util.Comparator.comparingLong;
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

    private boolean canUnbalanceWastageMigrate = true;

    public boolean isEnableMigrateOneUnderLoadHost() {
        return enableMigrateOneUnderLoadHost;
    }

    public void setEnableMigrateOneUnderLoadHost(boolean enableMigrateOneUnderLoadHost) {
        this.enableMigrateOneUnderLoadHost = enableMigrateOneUnderLoadHost;
    }

    public boolean isEnableMigrateMostUnbalanceWastageLoadHost() {
        return enableMigrateMostUnbalanceWastageLoadHost;
    }

    public void setEnableMigrateMostUnbalanceWastageLoadHost(boolean enableMigrateMostUnbalanceWastageLoadHost) {
        this.enableMigrateMostUnbalanceWastageLoadHost = enableMigrateMostUnbalanceWastageLoadHost;
    }

    public boolean enableMigrateMostUnbalanceWastageLoadHost = false;

    private boolean enableMigrateOneUnderLoadHost = false;
    public static final double DEF_UNDER_UTILIZATION_THRESHOLD = 0.3;
    public static final double DEF_RAM_UNDER_UTILIZATION_THRESHOLD = 0.3;
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
        this.hostsOverloaded = !overloadedHosts.isEmpty();
        printOverUtilizedHosts(overloadedHosts);
        saveAllocation();
        //????????????????????????????????????????????????????????????
        final Map<Vm, Host> migrationMap = getMigrationMapFromOverloadedHosts(overloadedHosts);
        updateMigrationMapFromUnderloadedHosts(overloadedHosts, migrationMap);
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
        final Map<Vm, Host> migrationMap)
    {
        final Set<Host> switchedOffHosts = getSwitchedOffHosts();

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
        //????????????????????????????????????????????????????????????????????????
        //?????????????????????????????????????????????????????????????????????????????????????????????host??????????????????
        ignoredSourceHosts.addAll(migrationMap.values());

        // overloaded + underloaded hosts
        final Set<Host> ignoredTargetHosts = getIgnoredHosts(overloadedHosts, switchedOffHosts);

        final int numberOfHosts = getHostList().size();

        this.hostsUnderloaded = false;
        this.canUnbalanceWastageMigrate = true;
        if(isEnableMigrateMostUnbalanceWastageLoadHost()){
            while(this.canUnbalanceWastageMigrate){
                //??????????????????host???????????????ignoredhost????????????????????????host?????????
                if (numberOfHosts == ignoredSourceHosts.size()) {
                    return;
//                    break;
                }
                final Host unbalanceHost = getUnbalanceWastegeHost(ignoredSourceHosts);
//                final Host underloadedHost = getUnderloadedHost(ignoredSourceHosts);
                if (unbalanceHost == Host.NULL) {
                    return;
                }

//                //???????????????????????????host???????????????????????????????????????????????????
//                if(!isHostUnderloaded(unbalanceHost)){
//                    return;
//                }

//                this.hostsUnderloaded = true;

                //?????????????????????????????????
//            printUnderUtilizedHosts(underloadedHost);

                LOGGER.warn("{}: VmAllocationPolicy: UnbalanceWastege and underload hosts: {}", getDatacenter().getSimulation().clockStr(), unbalanceHost);

                ignoredSourceHosts.add(unbalanceHost);
                ignoredTargetHosts.add(unbalanceHost);

                final List<? extends Vm> vmsToMigrateFromHost = getVmsToMigrateFromUnderUtilizedHost(unbalanceHost);
                if (!vmsToMigrateFromHost.isEmpty()) {
//                    System.out.println("?????????1");
                    logVmsToBeReallocated(unbalanceHost, vmsToMigrateFromHost);
                    final Map<Vm, Host> newVmPlacement = getNewVmPlacementFromUnderloadedHost(
                        vmsToMigrateFromHost,
                        ignoredTargetHosts,
                        unbalanceHost);

                    ignoredSourceHosts.addAll(extractHostListFromMigrationMap(newVmPlacement));
                    migrationMap.putAll(newVmPlacement);
                }
            }
        }else{
            while (true) {
                //??????????????????host???????????????ignoredhost????????????????????????host?????????
                if (numberOfHosts == ignoredSourceHosts.size()) {
                    break;
                }
                //????????????????????????????????????host??????
                final Host underloadedHost = getUnderloadedHost(ignoredSourceHosts);
                if (underloadedHost == Host.NULL) {
                    break;
                }
                this.hostsUnderloaded = true;

                //?????????????????????????????????
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
            if(isEnableMigrateMostUnbalanceWastageLoadHost()){
                LOGGER.info("{}: VmAllocationPolicy: VMs to be reallocated from the unbalanceWastege {}: {}",
                    getDatacenter().getSimulation().clockStr(), underloadedHost, getVmIds(migratingOutVms));
            }else{
                LOGGER.info("{}: VmAllocationPolicy: VMs to be reallocated from the underloaded {}: {}",
                    getDatacenter().getSimulation().clockStr(), underloadedHost, getVmIds(migratingOutVms));
            }
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
            "      Host %d (upper CPU threshold %f, current CPU utilization: %f,upper RAM threshold %f, current RAM utilization: %f)",
            host.getId(), this.getOverUtilizationThreshold(host), host.getCpuPercentUtilization(),this.getRamOverUtilizationThreshold(host),host.getRamPercentUtilization());
    }

    private String underloadedHostToString(final Host host) {
        return String.format(
            "      Host %d (lower CPU threshold %.2f, current CPU utilization: %.2f,lower RAM threshold %.2f, current RAM utilization: %.2f) ,current holding vm size: %d",
            host.getId(), this.getUnderUtilizationThreshold(), host.getCpuPercentUtilization(),this.getUnderRamUtilizationThreshold(),host.getRamPercentUtilization(),host.getVmList().size());
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
        double difference = powerAfterAllocation - host.getPowerModel().getPower();
        if (powerAfterAllocation > 0) {
            return difference;
        }

        return Double.MAX_VALUE;
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
        final Vm tempVm = new VmSimple(vm,true);
        HostSuitability suitability = host.createTemporaryVm(tempVm);
        if (!suitability.fully()) {
            System.out.println(vm+" ???????????????"+host+"?????????????????????????????????????????????????????????????????????");
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
        final boolean notOverloadedAfterAllocation = !isHostOverloaded(host);
        host.destroyTemporaryVm(tempVm);
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
        return cpuUsagePercent >= getOverUtilizationThreshold(host);
    }

    protected boolean isHostOverloaded(final Host host, final double cpuUsagePercent, final double ramUsagePercent){
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
        return cpuUsagePercent < getUnderUtilizationThreshold() || ramUsagePercent < getUnderRamUtilizationThreshold();
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
        return findHostForVm(vm, excludedHosts, host -> true).map(host -> host.setActive(true));
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
    private Map<Vm, Host> getMigrationMapFromOverloadedHosts(final Set<Host> overloadedHosts) {
        if(overloadedHosts.isEmpty()) {
            return new HashMap<>();
        }


        //?????????????????????????????????host???????????????
        final List<Vm> vmsToMigrate = getVmsToMigrateFromOverloadedHosts(overloadedHosts);
        //(??????)??????sort???????????????????????????
        sortByLoad(vmsToMigrate, getDatacenter().getSimulation().clock());
        final Map<Vm, Host> migrationMap = new HashMap<>();

        final StringBuilder builder = new StringBuilder();
        for (final Vm vm : vmsToMigrate) {
            Optional<Host> targethost = findHostForVm(vm, overloadedHosts);
            if(targethost.isPresent()){
                Host host = targethost.get();
                host.setCantShutdown(true);
                addVmToMigrationMap(migrationMap, vm, host);
                appendVmMigrationMsgToStringBuilder(builder, vm, host);
            }
            else{
//                System.out.println(getDatacenter().getSimulation().clockStr() + ": Vm "+ vm.getId()+" in source " + vm.getHost() +" can't find a host to place.now trying to awake a sleep host!");
//                //?????????vm????????????host??????????????????????????????????????????host???????????????
//                final Stream<Host> stream = switchedOffHosts.stream()
//                    .filter(host -> host.isSuitableForVm(vm))
//                    .filter(host -> isNotHostOverloadedAfterAllocation(host, vm));
//                Optional<Host> target = findHostForVmInternal(vm,stream);
//                Host host = null;
//                if(target.isPresent()){
//                    host = target.get();
//                    host.setActive(true);
//                    addVmToMigrationMap(migrationMap, vm, host);
//                    appendVmMigrationMsgToStringBuilder(builder, vm, host);
//                    System.out.println(getDatacenter().getSimulation().clockStr() + ": Host "+ host.getId()+" has been awake for Vm " +vm.getId()+" migration successful!");
//                    System.out.println();
//                }else{
//                    System.out.println(getDatacenter().getSimulation().clockStr() + ": ?????????????????????host??? "+ vm + " ??????????????????");
//                    System.out.println();
//                }
//                switchedOffHosts.remove(host);
                System.out.println(getDatacenter().getSimulation().clockStr() + ": ?????????????????????host??? "+ vm + " ??????????????????");
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
        //?????????vm????????????vm????????????????????????????????????bfd????????????
        sortByLoad(vmsToMigrate, getDatacenter().getSimulation().clock());
        for (final Vm vm : vmsToMigrate) {

            //????????????vm???cloudlet??????????????????????????????????????????????????????????????????????????????????????????
            if (vm.getCloudletScheduler().isEmpty()) {
                System.out.println("?????????2: "+vm+" "+vm.getHost());
                return new HashMap<>();
            }

            //try to find a target Host to place a VM from an underloaded Host that is not underloaded too
            //????????????????????????????????????????????????????????????
//            final Optional<Host> optional = findHostForVm(vm, excludedHosts, host -> !isHostUnderloaded(host));
            final Optional<Host> optional = findHostForVm(vm, excludedHosts, host -> true);
            //???????????????vm?????????host??????????????????map???????????????host???????????????
            if (!optional.isPresent()) {
                if(isEnableMigrateMostUnbalanceWastageLoadHost()){
                    for(Map.Entry<Vm,Host> entry:migrationMap.entrySet()){
                        entry.getValue().destroyTemporaryVm(entry.getKey());
                    }
                    System.out.println("????????????????????????host?????????vms,??????????????????");
                    this.canUnbalanceWastageMigrate = false;
                }else{
                    for(Map.Entry<Vm,Host> entry:migrationMap.entrySet()){
                        entry.getValue().destroyTemporaryVm(entry.getKey());
                    }
                    System.out.println("???????????????????????????vms");
                }
                //?????????
//                LOGGER.warn(
//                    "{}: VmAllocationPolicy: A new Host, which isn't also underloaded or won't be overloaded, couldn't be found to migrate {}. Migration of VMs from the underloaded {} cancelled.",
//                    getDatacenter().getSimulation().clockStr(), vm, vm.getHost());
                return new HashMap<>();
            }
            Host host = optional.get();
            addVmToMigrationMap(migrationMap, vm, host);
//            if(vm.getId() == 264){
//                System.out.println("mark: "+host+" "+host.getVmScheduler().getTotalAvailableMips()+" "+host.getRam().getAvailableResource()+" "+vm+" "+vm.getCurrentUtilizationMips()+" "+vm.getCurrentRequestedRam());
//            }
        }
        System.out.println(getDatacenter().getSimulation().clockStr()+" host "+underloadedHost.getId()+" ???vms???????????????????????????????????????????????????????????????????????????" );
//        underloadedHost.setActive(false);
        return migrationMap;
    }

    /**
     * Sort a given list of VMs by descending order of CPU utilization. ???cpu???????????????
     *
     * @param vmList the vm list to be sorted
     * @param simulationTime the simulation time to get the current CPU utilization for each Vm
     */
    protected void sortByCpuUtilization(final List<? extends Vm> vmList, final double simulationTime) {
        final Comparator<Vm> comparator = comparingDouble(vm -> vm.getTotalMipsCapacity() * vm.getCpuPercentUtilization());
        vmList.sort(comparator.reversed());
    }

    //???????????????
    protected void sortByMemoryDes(final List<? extends Vm> vmList, final double simulationTime) {
        final Comparator<Vm> comparator = comparingLong(Vm::getCurrentRequestedRam);
        vmList.sort(comparator);
    }

    protected void sortByUnbanlance(final List<? extends Vm> vmList, final double simulationTime){
        final Comparator<Vm> comparator = comparingDouble(vm->
            Math.abs(vm.getTotalMipsCapacity() * vm.getCpuPercentUtilization() / vm.getTotalMipsCapacity() - vm.getCurrentRequestedRam() / (double)vm.getRam().getCapacity())
        );
        vmList.sort(comparator);
    }

    protected void sortByUnbanlanceSystemLoadReverse(final List<? extends Vm> vmList, final double simulationTime){
        boolean systemLoad = systemLoad();
        final Comparator<Vm> comparator = comparingDouble(vm->
            systemLoad ?
                vm.getCurrentRequestedRam() / (double)vm.getRam().getCapacity() - vm.getTotalMipsCapacity() * vm.getCpuPercentUtilization() / vm.getTotalMipsCapacity() :
                vm.getTotalMipsCapacity() * vm.getCpuPercentUtilization() / vm.getTotalMipsCapacity() - vm.getCurrentRequestedRam() / (double)vm.getRam().getCapacity()
        );
        vmList.sort(comparator.reversed());
    }

    protected  void sortByVmCombineCpuAndRam(final List<? extends Vm> vmList, final double simulationTime){
        final Comparator<Vm> comparator = comparingDouble(vm -> vm.getTotalMipsCapacity() * vm.getCpuPercentUtilization() * vm.getCurrentRequestedRam());
        vmList.sort(comparator.reversed());
    }

    protected  void sortByLoad(final List<? extends Vm> vmList, final double simulationTime){
        final Comparator<Vm> comparator = comparingDouble(vm -> vm.getTotalMipsCapacity() * vm.getCpuPercentUtilization() / vm.getTotalMipsCapacity() + vm.getCurrentRequestedRam() / (double)vm.getRam().getCapacity());
        vmList.sort(comparator.reversed());
    }

    protected boolean systemLoad(){
        int cpu = 0,ram = 0;
        List<Host> hostList = getHostList();
        for(Host host:hostList){
            if(host.isActive()){
                if(host.getCpuMemLoad() == 0){
                    cpu++;
                }else if(host.getCpuMemLoad() == 1){
                    ram++;
                }
            }
        }
        return cpu > ram;
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
        host.setInFindMigrateVm(true);
//        System.out.println(" stop1: "+host+" ????????????vm???"+(host.getVmList().size()+host.getVmsMigratingIn().size())+" "+host.getCpuPercentUtilization());
        final List<Vm> vmsToMigrate = new LinkedList<>();
        while (true) {
            final Vm vm = getVmSelectionPolicy().getVmToMigrate(host);
            if(vm == Vm.NULL){
                System.out.println();
                System.out.println(" stop2: "+host+" ????????????vm???"+(host.getVmList().size()+host.getVmsMigratingIn().size())+" "+host.getCpuPercentUtilization());
                for(Vm nvm:host.getVmList()){
                    System.out.println(nvm+" "+nvm.getCpuPercentUtilization());
                }
            }
            if (Vm.NULL == vm || vm.getCloudletScheduler().isEmpty()) {
                break;
            }
            vmsToMigrate.add(vm);
            /*Temporarily destroys the selected VM into the overloaded Host so that
            the loop gets VMs from such a Host until it is not overloaded anymore.*/
            host.destroyTemporaryVm(vm);
            if (!isHostOverloaded(host)) {
                host.setInFindMigrateVm(false);
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
    //????????????????????????????????????vm??????iscreate
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

//    private boolean isShutdownOrFailed(final Host host) {
//        return !host.isActive() || host.isFailed();
//    }

    private boolean isShutdownOrFailed(final Host host) {
        return host.getCpuPercentUtilization() == 0.0 || host.getRamPercentUtilization() == 0.0 || host.isFailed();
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

    protected Host getUnbalanceWastegeHost(final Set<? extends Host> excludedHosts) {
        return this.getHostList().stream()
            .filter(host -> !excludedHosts.contains(host))
            .filter(Host::isActive)
            .filter(host -> host.getVmsMigratingIn().isEmpty())
            .filter(this::notAllVmsAreMigratingOut)
//            .filter(this::isHostUnderloaded)
            .max(comparingDouble(Host::avgResourceWastage))
            .orElse(Host.NULL);
    }

    protected double getHostCpuPercentRequested(final Host host) {
        return getHostTotalRequestedMips(host) / host.getTotalMipsCapacity();
    }

    protected double getHostCpuPercentUtilization(final Host host) {
        return getHostTotalUtilizationMips(host) / host.getTotalMipsCapacity();
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

    private double getHostTotalUtilizationMips(final Host host) {
        return host.getVmList().stream()
            .mapToDouble(Vm::getTotalCpuMipsUtilization)
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
            //????????????host???cpu??????ram?????????
//            double hostCpuPercentage = host.getCpuPercentUtilization();
//            double hostRamPercentage = host.getRamPercentUtilization();
//            if(hostCpuPercentage > hostRamPercentage){
//                host.setCpuMemLoad(0);
//            }else{
//                host.setCpuMemLoad(1);
//            }

//            if(hostCpuPercentage > 1.0 || hostRamPercentage > 1.0){
//                host.setTotalOver100Time(host.getTotalOver100Time() + 15);
//            }

//            //??????????????????????????????mips???ram??????
//            final Map<Vm, MipsShare> vmMipsShareMap = buildVmMipsReAllocations(host,hostCpuPercentage);
//            final Map<Vm, Long> vmLongMap = buildVmRamReAllocations(host,hostRamPercentage);
//            host.setVmMipsReAllocations(vmMipsShareMap);
//            host.setVmsRamReAllocations(vmLongMap);
//
//            final VmScheduler vmScheduler = host.getVmScheduler();
//            final ResourceProvisioner ramProvisioner = host.getRamProvisioner();
//
////            for(Vm vm:host.getVmList()){
////                System.out.println("mark10: "+vm+" "+vm.getHost()+" "+vm.getCurrentUtilizationMips());
////            }
//            //????????????host?????????mips??????,????????????????????????
//            vmScheduler.getAllocatedMipsMap().clear();
////            //??????????????????vm???mips????????????????????????????????????????????????????????????????????????????????????????????????????????????vm?????????
////            for (final Vm vm : host.getVmsMigratingIn()) {
////                vmScheduler.allocatePesForVm(vm, vm.getCurrentUtilizationMips());
////            }
//
//            //????????????host?????????ram??????
//            ramProvisioner.deallocateResourceForAllVms();

            List<Vm> removeDestroyVms = new ArrayList<>();
            for (final Vm vm : host.getVmList()) {

                //???????????????????????????????????????
                if(vm.isDestory() && vm.getCloudletScheduler().isEmpty()){
                    removeDestroyVms.add(vm);
                    continue;
                }

//                //???????????????vm?????????cpu?????????
//                if(vm.getHost().getId() == host.getId()){
//                    double percentage = vm.getCpuPercentUtilization();
////                    System.out.println(vm+" "+percentage+" "+vm.getCurrentUtilizationMips()+" "+vm.getCloudletScheduler().getRequestedCpuPercentUtilization(vm.getSimulation().clock()));
//                    vm.setCpuUtilizationBeforeMigration(percentage);
//
////                    forceMipsPlace(host, vmScheduler, vm);
//                }

//                placeReallocateVmsMips(host,vmScheduler,vm,vmMipsShareMap);
//
//                //(???????????????host???ram provisioner),????????????
////                forceRamtoPlace(host, ramProvisioner, vm);
//                placeReallocateVmsRam(host, ramProvisioner, vm, vmLongMap);

//                System.out.println("saveAllocation:"+vm+"  "+vm.getCurrentRequestedMips()+"  "+host.getVmScheduler().getAllocatedMips(vm)+ " "+vm.getCpuUtilizationBeforeMigration());

                /* TODO: this VM loop has a quadratic wost-case complexity (when
                    all Vms already in the VM list are migrating into this Host).
                *  Instead of looping over the vmsMigratingIn list for every VM,
                *  we could add a Host migratingIn attribute to the Vm.
                * Then, for every VM on the Host, we check this VM attribute
                * to see if the VM is migrating into the Host. */
                if (!host.getVmsMigratingIn().contains(vm)) {
                    savedAllocation.put(vm, host);
                }else{
                    System.out.println(vm+" ?????????????????????????????????????????????????????????");
                }
            }
//            for(final Vm vm:host.getVmsMigratingIn()){
//                //(???????????????host???ram provisioner),????????????
////                forceRamtoPlace(host, ramProvisioner, vm);
//                placeReallocateVmsRam(host, ramProvisioner, vm, vmLongMap);
//                placeReallocateVmsMips(host,vmScheduler,vm,vmMipsShareMap);
//            }
            for(Vm vm:removeDestroyVms){
                vm.setCreated(true);
                host.destroyVm(vm);
            }
//            System.out.println(host+ " vmsize:"+host.getVmList().size()+" mips:"+host.getVmScheduler().getTotalAvailableMips()+" ram:"+host.getRam().getAvailableResource());
        }
    }

    private Map<Vm,MipsShare> buildVmMipsReAllocations(Host host,double hostCpuPercentage){
        Map<Vm,MipsShare> VmMipsReAllocations = new HashMap<>();
        double vmsRequestMipsTotal = 0.0;
        double vmsMigratingRequestMipsTotal = 0.0;
        if(hostCpuPercentage >= 1.0){
            System.out.println("mark5: "+host.getSimulation().clock()+" "+host+" currentCpuPercentage is: "+hostCpuPercentage+ ",need to be reallocated    and currentRamPercentage is:" + host.getRamPercentUtilization());
            for(Vm vm:host.getVmList()){
                vmsRequestMipsTotal += vm.getCurrentUtilizationMips().totalMips();
            }
            for(Vm vm:host.getVmsMigratingIn()){
                if(!host.getVmList().contains(vm)){
                    vmsRequestMipsTotal += vm.getCurrentUtilizationMips().totalMips();
                }
            }
            double hostTotalMipsCapacity = host.getTotalMipsCapacity();
            for(Vm vm:host.getVmList()){
                double percentage = vm.getCurrentUtilizationMips().totalMips()/vmsRequestMipsTotal;
                long pes = vm.getNumberOfPes();
                MipsShare mipsShare = new MipsShare(pes,Math.floor(percentage * hostTotalMipsCapacity));
                VmMipsReAllocations.put(vm,mipsShare);
            }
            for(Vm vm:host.getVmsMigratingIn()){
                double percentage = vm.getCurrentUtilizationMips().totalMips()/vmsRequestMipsTotal;
                long pes = vm.getNumberOfPes();
                MipsShare mipsShare = new MipsShare(pes,Math.floor(percentage * hostTotalMipsCapacity));
                VmMipsReAllocations.put(vm,mipsShare);
            }
        }else{
            for(Vm vm:host.getVmList()){
                VmMipsReAllocations.put(vm,vm.getCurrentUtilizationMips());
            }
            for(Vm vm:host.getVmsMigratingIn()){
                VmMipsReAllocations.put(vm,vm.getCurrentUtilizationMips());
            }
        }
        return VmMipsReAllocations;
    }

    private Map<Vm,Long> buildVmRamReAllocations(Host host,double hostRamPercentage){
        Map<Vm,Long> VmsRamReAllocations = new HashMap<>();
        long vmsRequestRamTotal = 0;
        long hostRamCapacity = host.getRam().getCapacity();
        if(hostRamPercentage >= 1.0){
            System.out.println("mark6: "+host.getSimulation().clock()+" "+host+" currentRamPercentage is: "+hostRamPercentage+ ",need to be reallocated    and currentCpuPercentage is:" + host.getCpuPercentUtilization());
            for(Vm vm:host.getVmList()){
                vmsRequestRamTotal += vm.getCurrentRequestedRam();
            }
            for(final Vm vm:host.getVmsMigratingIn()){
                if(!host.getVmList().contains(vm))
                    vmsRequestRamTotal += vm.getCurrentRequestedRam();
            }
            for(Vm vm:host.getVmList()){
                //??????????????????ram??????????????????ram?????????ram?????????
                double percentage = (double)vm.getCurrentRequestedRam()/vmsRequestRamTotal;
                long requestRam = (long)(percentage * hostRamCapacity);
                VmsRamReAllocations.put(vm,requestRam);
            }
            for(final Vm vm:host.getVmsMigratingIn()){
                double percentage = (double)vm.getCurrentRequestedRam()/vmsRequestRamTotal;
                long requestRam = (long)(percentage * hostRamCapacity);
                VmsRamReAllocations.put(vm,requestRam);
            }
        }else{
            //????????????????????????????????????
            for(Vm vm:host.getVmList()){
                long requestRam = vm.getCurrentRequestedRam();
                VmsRamReAllocations.put(vm,requestRam);
            }
            for(final Vm vm:host.getVmsMigratingIn()){
                long requestRam = vm.getCurrentRequestedRam();
                VmsRamReAllocations.put(vm,requestRam);
            }
        }
        return VmsRamReAllocations;
    }

    private void placeReallocateVmsMips(Host host, VmScheduler vmScheduler, Vm vm,Map<Vm, MipsShare> vmMipsShareMap){
        if(vm.getCloudletScheduler().isEmpty()) return;
        MipsShare mipsShare = vmMipsShareMap.get(vm);
        allocateMipsAndForcePlace(host, vmScheduler, vm, mipsShare);
    }

    private void placeReallocateVmsRam(Host host, ResourceProvisioner ramProvisioner, Vm vm, Map<Vm, Long> vmLongMap){
        if(vm.getCloudletScheduler().isEmpty()) return;
        long allocateRam = vmLongMap.get(vm);
        allocateRamAndForcePlace(host, ramProvisioner, vm, allocateRam);
    }

    private void allocateRamAndForcePlace(Host host, ResourceProvisioner ramProvisioner, Vm vm, long allocateRam) {
        if(!ramProvisioner.allocateResourceForVm(vm, allocateRam)){
            LOGGER.error("VmAllocationPolicy: Couldn't update {} resource on {}, probably because it increase too many ram,now try to force it to place", vm, host);

            System.out.println("mark1:"+vm+" allocateRam:"+allocateRam+" CurrentCurrentRequestedRam:"+vm.getCurrentRequestedRam());
            System.out.println("mark2:"+host+" AvailableRam:"+host.getRam().getAvailableResource()+" AllocatedRam:"+host.getRamProvisioner().getAllocatedResourceForVm(vm));

            long avaliableResource = ramProvisioner.getAvailableResource();
            if(ramProvisioner.allocateResourceForVm(vm, avaliableResource)){
                LOGGER.error("VmAllocationPolicy: force update {} resource on {} successful", vm, host);
            }else{
                LOGGER.error("VmAllocationPolicy: force update {} resource on {} unfortunately failed !!! try to sovle it !!!", vm, host);
            }
        }
    }

    private void allocateMipsAndForcePlace(Host host, VmScheduler vmScheduler, Vm vm, MipsShare mipsShare) {
        if(!vmScheduler.allocatePesForVm(vm, mipsShare)){
            LOGGER.error("VmAllocationPolicy: Couldn't update {} resource on {}, probably because it increase too many mips,now try to force it to place", vm, host);

            System.out.println("mark1:"+vm+" currentMips:"+mipsShare+" CurrentRequestedMips:"+vm.getCurrentRequestedMips()+" CpuPercentUtilization:"+vm.getCpuPercentUtilization());
            System.out.println("mark2:"+host+" AvailableMips:"+host.getVmScheduler().getTotalAvailableMips()+" AllocatedMips:"+host.getVmScheduler().getAllocatedMips(vm));

            double availableMips = Math.floor(vmScheduler.getTotalAvailableMips());
            long pes = vm.getNumberOfPes();
            MipsShare tempMipsShare = new MipsShare(pes,availableMips/pes);
            if(!vmScheduler.allocatePesForVm(vm, tempMipsShare)){
                LOGGER.error("VmAllocationPolicy: force update {} resource on {} unfortunately failed !!! try to sovle it !!!", vm, host);
            }else{
                LOGGER.error("VmAllocationPolicy: force update {} resource on {} successful", vm, host);
            }
        }
    }


    private void forceMipsPlace(Host host, VmScheduler vmScheduler, Vm vm) {
        if(vm.getCloudletScheduler().isEmpty()) return;
        MipsShare currentMips = vm.getCurrentUtilizationMips();
//        if(vm.isInMigration() && vm.getHost().getId() != host.getId()){
//            currentMips = new MipsShare(currentMips.pes(),(vm.getCurrentRequestedMips().totalMips()-currentMips.totalMips())/currentMips.pes());
//        }
        allocateMipsAndForcePlace(host, vmScheduler, vm, currentMips);
    }

    //????????????????????????ram?????????host
    private void forceRamtoPlace(Host host, ResourceProvisioner ramProvisioner, Vm vm) {
        long allocatedRam = vm.getCurrentRequestedRam();
        allocateRamAndForcePlace(host, ramProvisioner, vm, allocatedRam);
//        System.out.println("mark4:"+host.getCpuPercentUtilization()+" "+host.getRamPercentUtilization());
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
            vm.setRestorePlace(true);
            final Host host = savedAllocation.get(vm);
            HostSuitability hostSuitability = host.createTemporaryVm(vm);
            if (hostSuitability.fully()){
                vm.setCreated(true);
            }
            //???????????????????????????????????????????????????
            else{
                LOGGER.error("VmAllocationPolicy: Couldn't restore {} on {}, now try to force it to restore", vm, host);
//                if(!hostSuitability.forRam()){
//                    vm.setForcePlace(true);
//                }
//                if(!hostSuitability.forPes()){
//                    vm.setCpuForcePlace(true);
//                }
//                if(host.createTemporaryVm(vm).fully()){
//                    vm.setCreated(true);
//                    LOGGER.error("VmAllocationPolicy:force {} to restore on {} successsful!",vm,host);
//                }else{
//                    LOGGER.error("VmAllocationPolicy: force restore {} on {} failed!", vm, host);
//                }
//                vm.setForcePlace(false);
//                vm.setCpuForcePlace(false);
//                vm.setForcePlace(false);
//                System.out.println();
            };
            vm.setRestorePlace(false);
        }

        //??????????????????????????????????????????????????????
        for (final Host host : getHostList()) {
            host.getVmsRamReAllocations().clear();
//            host.getVmMipsReAllocations().clear();
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
//            return host.getPowerModel().getPower(1.0);
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
        //???????????????????????????????????????????????????????????????
        final double requestedTotalMips = host.getSimulation().clock() < 0.2? vm.getTotalMipsCapacity(): vm.getCurrentUtilizationTotalMips();
        double hostUtilizationMips = host.getVmList().stream().mapToDouble(Vm::getTotalCpuMipsUtilization).sum();
//        double hostUtilizationMips = Math.floor(host.getCpuPercentUtilization() * host.getTotalMipsCapacity());
        if(host.getSimulation().clock() < 0.2){
            hostUtilizationMips = 0.0;
            for(Vm nvm:host.getVmList()){
                hostUtilizationMips += nvm.getTotalMipsCapacity();
            }
        }
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
//        return host.getTotalAllocatedMipsForVm(vm) / migrationOverhead;
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
