/*
 * Title: CloudSim Toolkit Description: CloudSim (Cloud Simulation) Toolkit for Modeling and
 * Simulation of Clouds Licence: GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009-2012, The University of Melbourne, Australia
 */
package org.cloudbus.cloudsim.hosts;

import org.cloudbus.cloudsim.core.*;
import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.power.models.PowerModelHost;
import org.cloudbus.cloudsim.provisioners.ResourceProvisioner;
import org.cloudbus.cloudsim.provisioners.ResourceProvisionerSimple;
import org.cloudbus.cloudsim.resources.*;
import org.cloudbus.cloudsim.schedulers.MipsShare;
import org.cloudbus.cloudsim.schedulers.vm.VmScheduler;
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerSpaceShared;
import org.cloudbus.cloudsim.util.Conversion;
import org.cloudbus.cloudsim.util.TimeUtil;
import org.cloudbus.cloudsim.vms.*;
import org.cloudsimplus.listeners.EventListener;
import org.cloudsimplus.listeners.HostEventInfo;
import org.cloudsimplus.listeners.HostUpdatesVmsProcessingEventInfo;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * A Host class that implements the most basic features of a Physical Machine
 * (PM) inside a {@link Datacenter}. It executes actions related to management
 * of virtual machines (e.g., creation and destruction). A host has a defined
 * policy for provisioning memory and bw, as well as an allocation policy for
 * PEs to {@link Vm virtual machines}. A host is associated to a Datacenter and
 * can host virtual machines.
 *
 * @author Rodrigo N. Calheiros
 * @author Anton Beloglazov
 * @since CloudSim Toolkit 1.0
 */
public class HostSimple implements Host, Serializable {
    private static long defaultRamCapacity = (long)Conversion.gigaToMega(10);
    private static long defaultBwCapacity = 1000;
    private static long defaultStorageCapacity = (long)Conversion.gigaToMega(500);
    private static int logLength = 12;

    public double getCPU_THRESHOLD() {
        return CPU_THRESHOLD;
    }
    public void setCPU_THRESHOLD(double CPU_THRESHOLD) {
        this.CPU_THRESHOLD = CPU_THRESHOLD;
    }
    public double getRAM_THRESHOLD() {
        return RAM_THRESHOLD;
    }
    public void setRAM_THRESHOLD(double RAM_THRESHOLD) {
        this.RAM_THRESHOLD = RAM_THRESHOLD;
    }

    double CPU_THRESHOLD = 1.0;
    double RAM_THRESHOLD = 1.0;

    public boolean isInFindMigrateVm() {
        return inFindMigrateVm;
    }

    public void setInFindMigrateVm(boolean inFindMigrateVm) {
        this.inFindMigrateVm = inFindMigrateVm;
    }

    private boolean inFindMigrateVm = false;

    public boolean isCantShutdown() {
        return CantShutdown;
    }

    public void setCantShutdown(boolean cantShutdown) {
        CantShutdown = cantShutdown;
    }

    private boolean CantShutdown = false;

    /** @see #getStateHistory() */
    private final List<HostStateHistoryEntry> stateHistory;

    /**@see #getPowerModel() */
    private PowerModelHost powerModel;

    public double getUtilizationMips() {
        return utilizationMips;
    }
    public void setUtilizationMips(double utilizationMips) {
        this.utilizationMips = utilizationMips;
    }
    private double utilizationMips = 0.0;
    public double getUtilizationOfCpu() {
        double utilization = getUtilizationMips() / getTotalMipsCapacity();
        if (utilization > 1 && utilization < 1.01) {
            utilization = 1;
        }
        return utilization;
    }

    public double getPreviousUtilizationMips() {
        return previousUtilizationMips;
    }
    public void setPreviousUtilizationMips(double previousUtilizationMips) {
        this.previousUtilizationMips = previousUtilizationMips;
    }
    public double getPreviousUtilizationOfCpu() {
        double utilization = getPreviousUtilizationMips() / getTotalMipsCapacity();
        if (utilization > 1 && utilization < 1.01) {
            utilization = 1;
        }
        return utilization;
    }

    public double getEnergyLinearInterpolation(double fromUtilization, double toUtilization, double time) {
        if (fromUtilization == 0) {
            return 0;
        }
        double fromPower = getPower(fromUtilization);
        double toPower = getPower(toUtilization);
        return (fromPower + (toPower - fromPower) / 2) * time;
    }

    protected double getPower(double utilization) {
        double power = 0;
        try {
            power = getPowerModel().getPower(utilization);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
        return power;
    }

    private double previousUtilizationMips = 0.0;
    private boolean previousActive = false;
    private double previousTime = -1;
    private double previousAllocated = 0;
    private double previousRequested = 0;
    private long previousAllocatedRam = 0;
    private long previousRequestedRam = 0;
    private double slaViolationTimePerHost = 0;

    public double getSlaViolationTimePerHost() {
        return slaViolationTimePerHost;
    }

    public void setSlaViolationTimePerHost(double slaViolationTimePerHost) {
        this.slaViolationTimePerHost = slaViolationTimePerHost;
    }

    /** @see #getId() */
    private long id;

    /** @see #isFailed() */
    private boolean failed;

    private boolean active;
    private boolean stateHistoryEnabled;

    public int getCpuMemLoad() {
        return cpuMemLoad;
    }

    public void setCpuMemLoad(int cpuMemLoad) {
        this.cpuMemLoad = cpuMemLoad;
    }

    private int cpuMemLoad = -1; //-1?????????????????????0??????cpu????????????1??????ram?????????

    public Map<Vm, MipsShare> getVmMipsReAllocations() {
        return VmMipsReAllocations;
    }

    public void setVmMipsReAllocations(Map<Vm, MipsShare> vmMipsReAllocations) {
        VmMipsReAllocations = vmMipsReAllocations;
    }

    public Map<Vm, Long> getVmsRamReAllocations() {
        return VmsRamReAllocations;
    }

    public void setVmsRamReAllocations(Map<Vm, Long> vmsRamReAllocations) {
        VmsRamReAllocations = vmsRamReAllocations;
    }

    Map<Vm,MipsShare> VmMipsReAllocations = new HashMap<>();
    Map<Vm,Long> VmsRamReAllocations = new HashMap<>();

    public double getIdlePower() {
        return idlePower;
    }

    public void setIdlePower(double idlePower) {
        this.idlePower = idlePower;
    }

    private double idlePower = 0.0;

    public int getTotalOver100Time() {
        return totalOver100Time;
    }

    public void setTotalOver100Time(int totalOver100Time) {
        this.totalOver100Time = totalOver100Time;
    }

    @Override
    public double getResourceWastage() {
        double xita = 0.0001;
        double hostCpuUtilization = this.getCpuPercentUtilization();
        double hostRamUtilization = this.getRamPercentUtilization();
        double hostRemindingCpuUtilization = 1 - hostCpuUtilization;
        double hostRemindingRamUtilization = 1 - hostRamUtilization;
        return (Math.abs(hostRemindingCpuUtilization - hostRemindingRamUtilization)+xita) / (hostCpuUtilization + hostRamUtilization);
    }

    int totalOver100Time = 0;

    /** @see #getStartTime() */
    private double startTime = -1;

    /** @see #getFirstStartTime() */
    private double firstStartTime = -1;

    /** @see #getShutdownTime() */
    private double shutdownTime;

    /** @see #getTotalUpTime() */
    private double totalUpTime;

    /** @see #getLastBusyTime() */
    private double lastBusyTime;

    /** @see #getIdleShutdownDeadline() */
    private double idleShutdownDeadline;

    private final Ram ram;
    private final Bandwidth bw;

    /** @see #getStorage() */
    private final HarddriveStorage disk;

    /** @see #getRamProvisioner() */
    private ResourceProvisioner ramProvisioner;

    /** @see #getBwProvisioner() */
    private ResourceProvisioner bwProvisioner;

    /** @see #getVmScheduler() */
    private VmScheduler vmScheduler;

    /** @see #getVmList() */
    private final List<Vm> vmList = new ArrayList<>();

    /** @see #getPeList() */
    private List<Pe> peList;

    /** @see #getVmsMigratingIn() */
    private final Set<Vm> vmsMigratingIn;

    /** @see #getVmsMigratingOut() */
    private final Set<Vm> vmsMigratingOut;

    /** @see #getDatacenter() */
    private Datacenter datacenter;

    /** @see #addOnUpdateProcessingListener(EventListener) */
    private final Set<EventListener<HostUpdatesVmsProcessingEventInfo>> onUpdateProcessingListeners;

    /** @see #addOnStartupListener(EventListener) (EventListener) */
    private final Set<EventListener<HostEventInfo>> onStartupListeners;

    /** @see #addOnShutdownListener(EventListener) (EventListener) */
    private final Set<EventListener<HostEventInfo>> onShutdownListeners;

    /** @see #getSimulation() */
    private Simulation simulation;

    /** @see #getResources() */
    private List<ResourceManageable> resources;

    private List<ResourceProvisioner> provisioners;
    private final List<Vm> vmCreatedList;

    /** @see #getFreePesNumber() */
    private int freePesNumber;

    /** @see #getFailedPesNumber() */
    private int failedPesNumber;

    private boolean lazySuitabilityEvaluation;
    protected HostResourceStats cpuUtilizationStats;

    public int getShutdownNumber() {
        return shutdownNumber;
    }

    public void setShutdownNumber(int shutdownNumber) {
        this.shutdownNumber = shutdownNumber;
    }

    private int shutdownNumber;

    /**
     * Creates and powers on a Host without a pre-defined ID,
     * 10GB of RAM, 1000Mbps of Bandwidth and 500GB of Storage.
     * It creates a {@link ResourceProvisionerSimple}
     * for RAM and Bandwidth. Finally, it sets a {@link VmSchedulerSpaceShared} as default.
     * The ID is automatically set when a List of Hosts is attached
     * to a {@link Datacenter}.
     *
     * @param peList the host's {@link Pe} list
     *
     * @see ChangeableId#setId(long)
     * @see #setRamProvisioner(ResourceProvisioner)
     * @see #setBwProvisioner(ResourceProvisioner)
     * @see #setVmScheduler(VmScheduler)
     * @see #setDefaultRamCapacity(long)
     * @see #setDefaultBwCapacity(long)
     * @see #setDefaultStorageCapacity(long)
     */
    public HostSimple(final List<Pe> peList) {
        this(peList, true);
    }

    /**
     * Creates a Host without a pre-defined ID,
     * 10GB of RAM, 1000Mbps of Bandwidth and 500GB of Storage
     * and enabling the host to be powered on or not.
     *
     * <p>It creates a {@link ResourceProvisionerSimple}
     * for RAM and Bandwidth. Finally, it sets a {@link VmSchedulerSpaceShared} as default.
     * The ID is automatically set when a List of Hosts is attached
     * to a {@link Datacenter}.</p>
     *
     * @param peList the host's {@link Pe} list
     * @param activate define the Host activation status: true to power on, false to power off
     *
     * @see ChangeableId#setId(long)
     * @see #setRamProvisioner(ResourceProvisioner)
     * @see #setBwProvisioner(ResourceProvisioner)
     * @see #setVmScheduler(VmScheduler)
     * @see #setDefaultRamCapacity(long)
     * @see #setDefaultBwCapacity(long)
     * @see #setDefaultStorageCapacity(long)
     */
    public HostSimple(final List<Pe> peList, final boolean activate) {
        this(defaultRamCapacity, defaultBwCapacity, defaultStorageCapacity, peList, activate);
    }

    /**
     * Creates and powers on a Host with the given parameters and a {@link VmSchedulerSpaceShared} as default.
     *
     * @param ramProvisioner the ram provisioner with capacity in Megabytes
     * @param bwProvisioner the bw provisioner with capacity in Megabits/s
     * @param storage the storage capacity in Megabytes
     * @param peList the host's PEs list
     *
     * @see #setVmScheduler(VmScheduler)
     */
    public HostSimple(
        final ResourceProvisioner ramProvisioner,
        final ResourceProvisioner bwProvisioner,
        final long storage,
        final List<Pe> peList)
    {
        this(ramProvisioner.getCapacity(), bwProvisioner.getCapacity(), storage, peList);
        setRamProvisioner(ramProvisioner);
        setBwProvisioner(bwProvisioner);
        setPeList(peList);
    }

    /**
     * Creates and powers on a Host without a pre-defined ID. It uses a {@link ResourceProvisionerSimple}
     * for RAM and Bandwidth and also sets a {@link VmSchedulerSpaceShared} as default.
     * The ID is automatically set when a List of Hosts is attached
     * to a {@link Datacenter}.
     *
     * @param ram the RAM capacity in Megabytes
     * @param bw the Bandwidth (BW) capacity in Megabits/s
     * @param storage the storage capacity in Megabytes
     * @param peList the host's {@link Pe} list
     *
     * @see ChangeableId#setId(long)
     * @see #setRamProvisioner(ResourceProvisioner)
     * @see #setBwProvisioner(ResourceProvisioner)
     * @see #setVmScheduler(VmScheduler)
     */
    public HostSimple(final long ram, final long bw, final long storage, final List<Pe> peList) {
        this(ram, bw, new HarddriveStorage(storage), peList);
    }

    public HostSimple(final long ram, final long bw, final HarddriveStorage storage, final List<Pe> peList) {
        this(ram, bw, storage, peList, true);
    }

    /**
     * Creates a Host without a pre-defined ID. It uses a {@link ResourceProvisionerSimple}
     * for RAM and Bandwidth and also sets a {@link VmSchedulerSpaceShared} as default.
     * The ID is automatically set when a List of Hosts is attached
     * to a {@link Datacenter}.
     *
     * @param ram the RAM capacity in Megabytes
     * @param bw the Bandwidth (BW) capacity in Megabits/s
     * @param storage the storage capacity in Megabytes
     * @param peList the host's {@link Pe} list
     * @param activate define the Host activation status: true to power on, false to power off
     *
     * @see ChangeableId#setId(long)
     * @see #setRamProvisioner(ResourceProvisioner)
     * @see #setBwProvisioner(ResourceProvisioner)
     * @see #setVmScheduler(VmScheduler)
     */
    public HostSimple(final long ram, final long bw, final long storage, final List<Pe> peList, final boolean activate) {
        this(ram, bw, new HarddriveStorage(storage), peList, activate);
    }

    private HostSimple(final long ram, final long bw, final HarddriveStorage storage, final List<Pe> peList, final boolean activate) {
        this.setId(-1);
        this.setSimulation(Simulation.NULL);
        this.setActive(activate);
        this.idleShutdownDeadline = DEF_IDLE_SHUTDOWN_DEADLINE;
        this.lazySuitabilityEvaluation = true;

        this.ram = new Ram(ram);
        this.bw = new Bandwidth(bw);
        this.disk = Objects.requireNonNull(storage);
        this.setRamProvisioner(new ResourceProvisionerSimple());
        this.setBwProvisioner(new ResourceProvisionerSimple());

        this.setVmScheduler(new VmSchedulerSpaceShared());
        this.setPeList(peList);
        this.setFailed(false);
        this.shutdownTime = -1;
        this.setDatacenter(Datacenter.NULL);

        this.onUpdateProcessingListeners = new HashSet<>();
        this.onStartupListeners = new HashSet<>();
        this.onShutdownListeners = new HashSet<>();
        this.cpuUtilizationStats = HostResourceStats.NULL;

        this.resources = new ArrayList<>();
        this.vmCreatedList = new ArrayList<>();
        this.provisioners = new ArrayList<>();
        this.vmsMigratingIn = new HashSet<>();
        this.vmsMigratingOut = new HashSet<>();
        this.powerModel = PowerModelHost.NULL;
        this.stateHistory = new LinkedList<>();
    }

    /**
     * Gets the Default RAM capacity (in MB) for creating Hosts.
     * This value is used when the RAM capacity is not given in a Host constructor.
     */
    public static long getDefaultRamCapacity() {
        return defaultRamCapacity;
    }

    /**
     * Sets the Default RAM capacity (in MB) for creating Hosts.
     * This value is used when the RAM capacity is not given in a Host constructor.
     */
    public static void setDefaultRamCapacity(final long defaultCapacity) {
        AbstractMachine.validateCapacity(defaultCapacity);
        defaultRamCapacity = defaultCapacity;
    }

    /**
     * Gets the Default Bandwidth capacity (in Mbps) for creating Hosts.
     * This value is used when the BW capacity is not given in a Host constructor.
     */
    public static long getDefaultBwCapacity() {
        return defaultBwCapacity;
    }

    /**
     * Sets the Default Bandwidth capacity (in Mbps) for creating Hosts.
     * This value is used when the BW capacity is not given in a Host constructor.
     */
    public static void setDefaultBwCapacity(final long defaultCapacity) {
        AbstractMachine.validateCapacity(defaultCapacity);
        defaultBwCapacity = defaultCapacity;
    }

    /**
     * Gets the Default Storage capacity (in MB) for creating Hosts.
     * This value is used when the Storage capacity is not given in a Host constructor.
     */
    public static long getDefaultStorageCapacity() {
        return defaultStorageCapacity;
    }

    /**
     * Sets the Default Storage capacity (in MB) for creating Hosts.
     * This value is used when the Storage capacity is not given in a Host constructor.
     */
    public static void setDefaultStorageCapacity(final long defaultCapacity) {
        AbstractMachine.validateCapacity(defaultCapacity);
        defaultStorageCapacity = defaultCapacity;
    }

    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override
    public double updateProcessing(final double currentTime) {
        if(vmList.isEmpty() && isIdleEnough(idleShutdownDeadline) && !isCantShutdown()){
            setActive(false);
        }

        setPreviousUtilizationMips(getUtilizationMips());
        setUtilizationMips(0);

        double nextSimulationDelay = Double.MAX_VALUE;
        for (int i = 0; i < vmList.size(); i++) {
            final Vm vm = vmList.get(i);
            final double delay = vm.updateProcessing(currentTime, vmScheduler.getAllocatedMips(vm));
            nextSimulationDelay = delay > 0 ? Math.min(delay, nextSimulationDelay) : nextSimulationDelay;
        }

        //?????????????????????vm?????????mips
        double hostCpuPercentage = getCpuPercentUtilization();
        double hostRamPercentage = getRamPercentUtilization();
        if(hostCpuPercentage > 1.0){
            System.out.println("mark5: "+getSimulation().clock()+" "+this+" currentCpuPercentage is: "+hostCpuPercentage+ ",need to be reallocated    and currentRamPercentage is:" + getRamPercentUtilization());
        }
        vmScheduler.deallocatePesForAllVms();
        double vmsRequestMipsTotal = 0.0;
        double vmsAllocatedMipsTotal = 0.0;
        for (Vm vm : getVmList()) {
            vmScheduler.allocatePesForVm(vm, vm.getCurrentUtilizationMips());
        }

        //?????????????????????vm?????????ram
        ramProvisioner.deallocateResourceForAllVms();
        VmsRamReAllocations.clear();
        long vmsRequestRamTotal = 0;
        long vmsAllocatedRamTotal = 0;
        long hostRamCapacity = getRam().getCapacity();
        if(hostRamPercentage > 1.0){
            System.out.println("mark6: "+getSimulation().clock()+" "+ this +" currentRamPercentage is: "+hostRamPercentage+ ",need to be reallocated    and currentCpuPercentage is:" + getCpuPercentUtilization());
            for(Vm vm:getVmList()){
                vmsRequestRamTotal += vm.getCurrentRequestedRam();
            }
            for(final Vm vm:getVmsMigratingIn()){
                if(!getVmList().contains(vm))
                    vmsRequestRamTotal += vm.getCurrentRequestedRam();
            }
            double factor = (double) hostRamCapacity / vmsRequestRamTotal;
            for(Vm vm:getVmList()){
                long requestRam = (long)(vm.getCurrentRequestedRam() * factor);
                VmsRamReAllocations.put(vm,requestRam);
                ramProvisioner.allocateResourceForVm(vm, requestRam);
            }
            for(final Vm vm:getVmsMigratingIn()){
                long requestRam = (long)(vm.getCurrentRequestedRam() * factor);
                VmsRamReAllocations.put(vm,requestRam);
                ramProvisioner.allocateResourceForVm(vm, requestRam);
            }
        }else{
            for(Vm vm:getVmList()){
                long requestRam = vm.getCurrentRequestedRam();
                VmsRamReAllocations.put(vm,requestRam);
                ramProvisioner.allocateResourceForVm(vm, requestRam);
            }
            for(final Vm vm:getVmsMigratingIn()){
                long requestRam = vm.getCurrentRequestedRam();
                VmsRamReAllocations.put(vm,requestRam);
                ramProvisioner.allocateResourceForVm(vm, requestRam);
            }
        }

        for (int i = 0; i < vmList.size(); i++) {
            final Vm vm = vmList.get(i);

            double AllocatedMips = vmScheduler.getTotalAllocatedMipsForVm(vm);
            long AllocatedRam = ramProvisioner.getAllocatedResourceForVm(vm);
            double RequestedMips = vm.getCurrentUtilizationMips().totalMips();
            long RequestedRam = vm.getCurrentRequestedRam();

            if(!getVmsMigratingIn().contains(vm)){

                //??????vm????????????
                vm.addStateHistoryEntry(new VmStateHistoryEntry(
                    currentTime, AllocatedMips, RequestedMips, (vm.isInMigration() && !getVmsMigratingIn().contains(vm))));

                //????????????vm?????????????????????
//                underallocatedMigration(vm, AllocatedMips, AllocatedRam, RequestedMips, RequestedRam, currentTime);

                if(vm.isInMigration()){
                    AllocatedMips /= 0.9;
                }
            }
            //??????host mips?????????????????????
            vmsRequestMipsTotal += RequestedMips;
            vmsAllocatedMipsTotal += AllocatedMips;
            //??????host ram?????????????????????
            vmsRequestRamTotal += RequestedRam;
            vmsAllocatedRamTotal += AllocatedRam;

            setUtilizationMips(getUtilizationMips() + AllocatedMips);
        }
        if(getUtilizationMips() > getTotalMipsCapacity()){
            setUtilizationMips(getTotalMipsCapacity());
        }

        //?????????????????????sla????????????
//        SlaTimePerActiveHost(currentTime,getUtilizationMips(),vmsRequestMipsTotal,vmsAllocatedRamTotal,vmsRequestRamTotal);
        if(stateHistoryEnabled){
            addStateHistoryEntry(currentTime, getUtilizationMips() ,vmsRequestMipsTotal,vmsAllocatedRamTotal,vmsRequestRamTotal,(getUtilizationMips()  > 0));
        }
        notifyOnUpdateProcessingListeners(currentTime);
//        addStateHistory(currentTime);

        if (!vmList.isEmpty()) {
            lastBusyTime = currentTime;
        }

        return nextSimulationDelay;
    }

    private void SlaTimePerActiveHost(double currentTime,double vmsAllocatedMipsTotal,double vmsRequestMipsTotal,long vmsAllocatedRamTotal,long vmsRequestRamTotal){
        if (previousTime != -1 && previousActive) {
            double timeDiff = currentTime - previousTime;;
            if (previousAllocated < previousRequested || previousAllocatedRam < previousRequestedRam) {
                slaViolationTimePerHost += timeDiff;
            }
        }

        previousTime = currentTime;
        previousAllocated = vmsAllocatedMipsTotal;
        previousRequested = vmsRequestMipsTotal;
        previousAllocatedRam = vmsAllocatedRamTotal;
        previousRequestedRam = vmsRequestRamTotal;
        previousActive = vmsAllocatedMipsTotal > 0;
    }

    private void underallocatedMigration(Vm vm,double AllocatedMips, long AllocatedRam, double RequestedMips, long RequestedRam,double currentTime){

        double previousTime = vm.getPreviousTime();
        double timeDiff = currentTime - previousTime;

        //???????????????vm??????????????????mips
        boolean isPreviousInMigration = vm.isPreviousIsInMigration();
        double previousAllocated = vm.getPreviousAllocated();
        double previousRequested = vm.getPreviousRequested();
        BigDecimal b1 = new BigDecimal(Double.toString(previousAllocated));
        BigDecimal b2 = new BigDecimal(Double.toString(previousRequested));

        //??????vm?????????????????????
        if(previousTime != -1){
            if(previousAllocated < previousRequested){
                if(isPreviousInMigration){
                    vm.setVmUnderAllocatedDueToMigration(vm.getVmUnderAllocatedDueToMigration() + (b2.subtract(b1).doubleValue()) * timeDiff);
                }
            }
        }

//        //??????????????????ram?????????
//        BigDecimal b3 = new BigDecimal(Double.toString(AllocatedRam));
//        BigDecimal b4 = new BigDecimal(Double.toString(RequestedRam));
//        if(AllocatedRam < RequestedRam){
//            if(isInMigration){
//                vm.setVmRamUnderAllocatedDueToMigration(vm.getVmRamUnderAllocatedDueToMigration() + (b4.subtract(b3).doubleValue()) * timeDiff);
//            }
//        }

        //????????????vm????????????mips
        vm.setTotalrequestUtilization(vm.getTotalrequestUtilization() + AllocatedMips * timeDiff);

        //????????????????????????mips??????????????????????????????
        vm.setPreviousTime(currentTime);
        vm.setPreviousIsInMigration(vm.isInMigration() && !getVmsMigratingIn().contains(vm));
        vm.setPreviousAllocated(AllocatedMips);
        vm.setPreviousRequested(RequestedMips);
    }

    private void notifyOnUpdateProcessingListeners(final double nextSimulationTime) {
        onUpdateProcessingListeners.forEach(l -> l.update(HostUpdatesVmsProcessingEventInfo.of(l,this, nextSimulationTime)));
    }

    @Override
    public HostSuitability createVm(final Vm vm) {
        final HostSuitability suitability = createVmInternal(vm);
        if(suitability.fully()) {
            addVmToCreatedList(vm);
            vm.setHost(this);
            vm.setCreated(true);
            vm.notifyOnHostAllocationListeners();
            vm.setStartTime(getSimulation().clock());
        }

        return suitability;
    }

    @Override
    public HostSuitability createTemporaryVm(final Vm vm) {
        return createVmInternal(vm);
    }

    private HostSuitability createVmInternal(final Vm vm) {
        if(vm instanceof VmGroup){
            return new HostSuitability("Just internal VMs inside a VmGroup can be created, not the VmGroup itself.");
        }

        final HostSuitability suitability = allocateResourcesForVm(vm, false);
        if(suitability.fully()){
            vmList.add(vm);
        }

        return suitability;
    }

    /**
     * Try to allocate all resources that a VM requires (Storage, RAM, BW and MIPS) to be placed at this Host.
     *
     * @param vm the VM to try allocating resources to
     * @param inMigration If the VM is migrating into the Host or it is being just created for the first time.
     * @return a {@link HostSuitability} to indicate if the Vm was placed into the host or not
     * (if the Host doesn't have enough resources to allocate the Vm)
     */
    private HostSuitability allocateResourcesForVm(final Vm vm, final boolean inMigration){
        HostSuitability suitability;
        if(!vm.isRestorePlace()){
            suitability = isSuitableForVm(vm, inMigration, true);
        }else{
            suitability = new HostSuitability();
            suitability.setForStorage(true);
            suitability.setForRam(true);
            suitability.setForBw(true);
            suitability.setForPes(true);
        }
        if(!suitability.fully()) {
            return suitability;
        }
        if(!(vm.isInMigration() && !inMigration)){
            vm.setInMigration(inMigration);
        }
        allocateResourcesForVm(vm);
        return suitability;
    }

    private void allocateResourcesForVm(Vm vm) {
//        if(!vm.isForcePlace()){
//            if(vm.isRestorePlace()){
//                ramProvisioner.allocateResourceForVm(vm, getVmsRamReAllocations().get(vm));
//            }else{
//                ramProvisioner.allocateResourceForVm(vm, vm.getCurrentRequestedRam());
//            }
//        }else{
//            long leftRam = ramProvisioner.getAvailableResource();
//            ramProvisioner.allocateResourceForVm(vm,leftRam);
//        }
//        if(!vm.isCpuForcePlace()){
//            if(vm.isRestorePlace()){
//                vmScheduler.allocatePesForVm(vm, getVmMipsReAllocations().get(vm));
//            }else{
//                vmScheduler.allocatePesForVm(vm, vm.getCurrentUtilizationMips());
//            }
//        }else{
//            double leftMips = vmScheduler.getTotalAvailableMips();
//            long pes = vm.getNumberOfPes();
//            vmScheduler.allocatePesForVm(vm,new MipsShare(pes,leftMips/pes));
//        }
        if(vm.isRestorePlace()){
            ramProvisioner.allocateResourceForVm(vm,VmsRamReAllocations.get(vm));
        }else{
            ramProvisioner.allocateResourceForVm(vm, vm.getCurrentRequestedRam());
        }
        vmScheduler.allocatePesForVm(vm, vm.getCurrentUtilizationMips());
        bwProvisioner.allocateResourceForVm(vm, vm.getCurrentRequestedBw());
        disk.getStorage().allocateResource(vm.getStorage());
    }

    private void logAllocationError(
        final boolean showFailureLog, final Vm vm,
        final boolean inMigration, final String resourceUnit,
        final Resource pmResource, final Resource vmRequestedResource)
    {
        if(!showFailureLog){
            return;
        }

        final String migration = inMigration ? "VM Migration" : "VM Creation";
        final String msg = pmResource.getAvailableResource() > 0 ? "just "+pmResource.getAvailableResource()+" " + resourceUnit : "no amount";
        if(vm.getId() == -1){
            LOGGER.error(
                "{}: {}: [{}] Allocation of {} to {} failed due to lack of {}. Required {} but there is {} available.",
                simulation.clockStr(), getClass().getSimpleName(), migration, vm, this,
                pmResource.getClass().getSimpleName(), vm.getRam().getCapacity(), msg);
        }else{
            LOGGER.error(
                "{}: {}: [{}] Allocation of {} to {} failed due to lack of {}. Required {} but there is {} available.",
                simulation.clockStr(), getClass().getSimpleName(), migration, vm, this,
                pmResource.getClass().getSimpleName(), vm.getCurrentRequestedRam(), msg);
        }
    }

    @Override
    public void reallocateMigratingInVms() {
        for (final Vm vm : getVmsMigratingIn()) {
            vm.setRestorePlace(true);
            //???????????? ??????vm????????????targethost??????????????????
            if (!vmList.contains(vm)) {
                vmList.add(vm);
            }
            allocateResourcesForVm(vm);
            vm.setRestorePlace(false);
        }
    }

    @Override
    public boolean isSuitableForVm(final Vm vm) {
//        System.out.println(vm.getSimulation().clockStr()+" : before: "+vm+" ?????????ram????????? "+vm.getCurrentRequestedRam());
        return getSuitabilityFor(vm).fully();
    }

    @Override
    public HostSuitability getSuitabilityFor(final Vm vm) {
        return isSuitableForVm(vm, false, false);
    }

    /**
     * Checks if the host is suitable for vm
     * (if it has enough resources to attend the VM)
     * and the Host is not failed.
     *
     * @param vm the VM to check
     * @param inMigration If the VM is migrating into the Host or it is being just created for the first time,
     *                    in this case, just for logging purposes.
     * @param showFailureLog indicates if a error log must be shown when the Host is not suitable
     * @return a {@link HostSuitability} object that indicate for which resources the Host is suitable or not for the given VM
     */
    private HostSuitability isSuitableForVm(final Vm vm, final boolean inMigration, final boolean showFailureLog) {
        final HostSuitability suitability = new HostSuitability();

        suitability.setForStorage(disk.isAmountAvailable(vm.getStorage()));
        if (!suitability.forStorage()) {
            logAllocationError(showFailureLog, vm, inMigration, "MB", this.getStorage(), vm.getStorage());
            if(lazySuitabilityEvaluation)
                return suitability;
        }

        suitability.setForRam(ramProvisioner.isSuitableForVm(vm, vm.getRam()));
        if (!suitability.forRam()) {
            logAllocationError(showFailureLog, vm, inMigration, "MB", this.getRam(), vm.getRam());
            if(lazySuitabilityEvaluation)
                return suitability;
        }

        suitability.setForBw(bwProvisioner.isSuitableForVm(vm, vm.getBw()));
        if (!suitability.forBw()) {
            logAllocationError(showFailureLog, vm, inMigration, "Mbps", this.getBw(), vm.getBw());
            if(lazySuitabilityEvaluation)
                return suitability;
        }

        suitability.setForPes(vmScheduler.isSuitableForVm(vm));
        return suitability;
    }

    @Override
    public boolean isActive() {
        return this.active;
    }

    @Override
    public boolean hasEverStarted() {
        return this.firstStartTime > -1;
    }

    @Override
    public final Host setActive(final boolean activate) {
        if(this.active == activate){
            return this;
        }

        if(isFailed() && activate){
            throw new IllegalStateException("The Host is failed and cannot be activated.");
        }

        final boolean wasActive = this.active;

        if(activate && !this.active) {
            setStartTime(getSimulation().clock());
        } else if(!activate && this.active){
            setShutdownTime(getSimulation().clock());
            shutdownNumber++;
        }

        this.active = activate;
        notifyStartupOrShutdown(activate, wasActive);
        return this;
    }

    /**
     * Notifies registered listeners about host start up or shutdown,
     * then prints information when the Host starts up or shuts down.
     * @param activate the activation value that is being requested to set
     * @param wasActive the previous value of the {@link #active} attribute
     *                  (before being updated)
     * @see #setActive(boolean)
     */
    private void notifyStartupOrShutdown(final boolean activate, final boolean wasActive) {
        if(simulation == null || !simulation.isRunning() ) {
            return;
        }

        if(activate && !wasActive){
            LOGGER.info("{}: {} is being powered on.", getSimulation().clockStr(), this);
            onStartupListeners.forEach(l -> l.update(HostEventInfo.of(l, this, simulation.clock())));
        }
        else if(!activate && wasActive){
            final String reason = isIdleEnough(idleShutdownDeadline) ? " after becoming idle" : "";
            LOGGER.info("{}: {} is being powered off{}.", getSimulation().clockStr(), this, reason);
            onShutdownListeners.forEach(l -> l.update(HostEventInfo.of(l, this, simulation.clock())));
        }
    }

    @Override
    public void destroyVm(final Vm vm) {
        if(!vm.isCreated()){
            return;
        }

        destroyVmInternal(vm);
        vm.notifyOnHostDeallocationListeners(this);
        vm.setStopTime(getSimulation().clock());
    }

    @Override
    public void destroyTemporaryVm(final Vm vm) {
        destroyVmInternal(vm);
    }

    private void destroyVmInternal(final Vm vm) {
        deallocateResourcesOfVm(requireNonNull(vm));
        vmList.remove(vm);
        vm.getBroker().getVmExecList().remove(vm);
    }

    /**
     * Deallocate all resources that a VM was using.
     *
     * @param vm the VM
     */
    protected void deallocateResourcesOfVm(final Vm vm) {
        vm.setCreated(false);
        ramProvisioner.deallocateResourceForVm(vm);
        bwProvisioner.deallocateResourceForVm(vm);
        vmScheduler.deallocatePesFromVm(vm);
        disk.getStorage().deallocateResource(vm.getStorage());
    }

    @Override
    public void destroyAllVms() {
        deallocateResourcesOfAllVms();
        for (final Vm vm : vmList) {
            vm.setCreated(false);
            disk.getStorage().deallocateResource(vm.getStorage());
        }

        vmList.clear();
    }

    @Override
    public Host addOnStartupListener(final EventListener<HostEventInfo> listener) {
        if(EventListener.NULL.equals(listener)){
            return this;
        }

        onStartupListeners.add(Objects.requireNonNull(listener));
        return this;
    }

    @Override
    public boolean removeOnStartupListener(final EventListener<HostEventInfo> listener) {
        return onStartupListeners.remove(listener);
    }

    @Override
    public Host addOnShutdownListener(final EventListener<HostEventInfo> listener) {
        if(EventListener.NULL.equals(listener)){
            return this;
        }

        onShutdownListeners.add(Objects.requireNonNull(listener));
        return this;
    }

    @Override
    public boolean removeOnShutdownListener(final EventListener<HostEventInfo> listener) {
        return onShutdownListeners.remove(listener);
    }

    /**
     * Deallocate all resources that all VMs were using.
     */
    protected void deallocateResourcesOfAllVms() {
        ramProvisioner.deallocateResourceForAllVms();
        bwProvisioner.deallocateResourceForAllVms();
        vmScheduler.deallocatePesForAllVms();
    }

    /**
     * {@inheritDoc}
     * @return {@inheritDoc}
     * @see #getWorkingPesNumber()
     * @see #getFreePesNumber()
     * @see #getFailedPesNumber()
     */
    @Override
    public long getNumberOfPes() {
        return peList.size();
    }

    /**
     * Gets the MIPS share of each Pe that is allocated to a given VM.
     *
     * @param vm the vm
     * @return an array containing the amount of MIPS of each pe that is available to the VM
     */
    protected MipsShare getAllocatedMipsForVm(final Vm vm) {
        return vmScheduler.getAllocatedMips(vm);
    }

    @Override
    public double getMips() {
        return peList.stream().mapToDouble(Pe::getCapacity).findFirst().orElse(0);
    }

    @Override
    public double getTotalMipsCapacity() {
        return peList.stream()
                     .filter(Pe::isWorking)
                     .mapToDouble(Pe::getCapacity)
                     .sum();
    }

    @Override
    public double getTotalAvailableMips() {
        return vmScheduler.getTotalAvailableMips();
    }

    @Override
    public double getTotalAllocatedMips() {
        return getTotalMipsCapacity() - getTotalAvailableMips();
    }

    @Override
    public double getTotalAllocatedMipsForVm(final Vm vm) {
        return vmScheduler.getTotalAllocatedMipsForVm(vm);
    }

    @Override
    public Resource getBw() {
        return bwProvisioner.getResource();
    }

    @Override
    public Resource getRam() {
        return ramProvisioner.getResource();
    }

    @Override
    public FileStorage getStorage() {
        return disk;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public final void setId(long id) {
        this.id = id;
    }

    @Override
    public ResourceProvisioner getRamProvisioner() {
        return ramProvisioner;
    }

    @Override
    public final Host setRamProvisioner(final ResourceProvisioner ramProvisioner) {
        checkSimulationIsRunningAndAttemptedToChangeHost("RAM");
        this.ramProvisioner = requireNonNull(ramProvisioner);
        this.ramProvisioner.setResource(ram);
        return this;
    }

    private void checkSimulationIsRunningAndAttemptedToChangeHost(final String resourceName) {
        if(simulation.isRunning()){
            throw new IllegalStateException("It is not allowed to change a Host's "+resourceName+" after the simulation started.");
        }
    }

    @Override
    public ResourceProvisioner getBwProvisioner() {
        return bwProvisioner;
    }

    @Override
    public final Host setBwProvisioner(final ResourceProvisioner bwProvisioner) {
        checkSimulationIsRunningAndAttemptedToChangeHost("BW");
        this.bwProvisioner = requireNonNull(bwProvisioner);
        this.bwProvisioner.setResource(bw);
        return this;
    }

    @Override
    public VmScheduler getVmScheduler() {
        return vmScheduler;
    }

    @Override
    public final Host setVmScheduler(final VmScheduler vmScheduler) {
        this.vmScheduler = requireNonNull(vmScheduler);
        vmScheduler.setHost(this);
        return this;
    }

    @Override
    public double getStartTime() {
        return startTime;
    }

    @Override
    public double getFirstStartTime(){
        return firstStartTime;
    }

    @Override
    public Host setStartTime(final double startTime) {
        if(startTime < 0){
            throw new IllegalArgumentException("Host start time cannot be negative");
        }

        this.startTime = Math.floor(startTime);
        if(firstStartTime == -1){
            firstStartTime = this.startTime;
        }

        //If the Host is being activated or re-activated, the shutdown time is reset
        this.shutdownTime = -1;
        return this;
    }

    @Override
    public double getShutdownTime() {
        return shutdownTime;
    }

    @Override
    public void setShutdownTime(final double shutdownTime) {
        if(shutdownTime < 0){
            throw new IllegalArgumentException("Host shutdown time cannot be negative");
        }

        this.shutdownTime = Math.floor(shutdownTime);
        this.totalUpTime += getUpTime();
    }

    @Override
    public double getUpTime() {
        return active ? simulation.clock() - startTime : shutdownTime - startTime;
    }

    @Override
    public double getTotalUpTime() {
        return totalUpTime + (active ? getUpTime() : 0);
    }

    @Override
    public double getUpTimeHours() {
        return TimeUtil.secondsToHours(getUpTime());
    }

    @Override
    public double getTotalUpTimeHours() {
        return TimeUtil.secondsToHours(getTotalUpTime());

    }

    @Override
    public double getIdleShutdownDeadline() {
        return idleShutdownDeadline;
    }

    @Override
    public Host setIdleShutdownDeadline(final double deadline) {
        this.idleShutdownDeadline = deadline;
        return this;
    }

    @Override
    public List<Pe> getPeList() {
        return peList;
    }

    /**
     * Sets the PE list.
     *
     * @param peList the new pe list
     */
    private void setPeList(final List<Pe> peList) {
        requireNonNull(peList);
        checkSimulationIsRunningAndAttemptedToChangeHost("List of PE");
        this.peList = peList;

        long peId = this.peList.stream().filter(pe -> pe.getId() > 0).mapToLong(Pe::getId).max().orElse(-1);
        final List<Pe> pesWithoutIds = this.peList.stream().filter(pe -> pe.getId() < 0).collect(toList());
        for(final Pe pe: pesWithoutIds){
            pe.setId(++peId);
        }

        failedPesNumber = 0;
        setPeStatus(peList, Pe.Status.FREE);
        freePesNumber = peList.size();

    }

    @Override
    public <T extends Vm> List<T> getVmList() {
        return (List<T>) Collections.unmodifiableList(vmList);
    }

    @Override
    public <T extends Vm> List<T> getVmCreatedList() {
        return (List<T>) Collections.unmodifiableList(vmCreatedList);
    }

    protected void addVmToList(final Vm vm){
        vmList.add(requireNonNull(vm));
    }

    protected void addVmToCreatedList(final Vm vm){
        vmCreatedList.add(requireNonNull(vm));
    }

    @Override
    public boolean isFailed() {
        return failed;
    }

    @Override
    public final boolean setFailed(final boolean failed) {
        this.failed = failed;
        final Pe.Status newStatus = failed ? Pe.Status.FAILED : Pe.Status.FREE;
        setPeStatus(peList, newStatus);

        /*Just changes the active state when the Host is set to active.
        * In other situations, the active status must remain as it was.
        * For example, if the host was inactive and now it's set to failed,
        * it must remain inactive.*/
        if(failed && this.active){
            this.active = false;
        }

        return true;
    }

    /**
     * Sets the status of a given (sub)list of {@link Pe} to a new status.
     * @param peList the (sub)list of {@link Pe} to change the status
     * @param newStatus the new status
     */
    public final void setPeStatus(final List<Pe> peList, final Pe.Status newStatus){
        /*For performance reasons, stores the number of free and failed PEs
        instead of iterating over the PE list every time to find out.*/
        for (final Pe pe : peList) {
            updatePeStatus(pe, newStatus);
        }
    }

    private void updatePeStatus(final Pe pe, final Pe.Status newStatus) {
        if(pe.getStatus() != newStatus) {
            updateFailedAndFreePesNumber(pe.getStatus(), false);
            updateFailedAndFreePesNumber(newStatus, true);
            pe.setStatus(newStatus);
        }
    }

    /**
     * Update the number of Failed and Free PEs.
     * @param newStatus the new status which is being set for a PE
     * @param increment true to increment the numbers of Failed and Free PEs to 1, false to decrement
     */
    private void updateFailedAndFreePesNumber(final Pe.Status newStatus, final boolean increment) {
        final int i = increment ? 1 : -1;
        switch (newStatus) {
            case FAILED: this.failedPesNumber += i; break;
            case FREE:  this.freePesNumber += i; break;
        }
    }

    @Override
    public <T extends Vm> Set<T> getVmsMigratingIn() {
        return (Set<T>)vmsMigratingIn;
    }

    @Override
    public boolean hasMigratingVms(){
        return !(vmsMigratingIn.isEmpty() && vmsMigratingOut.isEmpty());
    }

    @Override
    public boolean addMigratingInVm(final Vm vm) {
        /* TODO: Instead of keeping a list of VMs which are migrating into a Host,
        *  which requires searching in such a list every time a VM is requested to be migrated
        *  to that Host (to check if it isn't migrating to that same host already),
        *  we can add a migratingHost attribute to Vm, so that the worst time complexity
        *  will change from O(N) to a constant time O(1). */
        if (vmsMigratingIn.contains(vm)) {
            return false;
        }

        vmsMigratingIn.add(vm);
        vmList.add(vm);
        //?????????????????????????????????????????????????????????vmlist


        if(!allocateResourcesForVm(vm, true).fully()){
            System.out.println(vm+" ?????????mips???"+vm.getCurrentUtilizationMips().totalMips()+"  ?????????ram:"+vm.getCurrentRequestedRam()+" cpu????????????"+vm.getCpuPercentUtilization()+" beforeMigration:"+vm.getCpuPercentUtilization());
            System.out.println(this+" ?????????mips???"+this.getVmScheduler().getTotalAvailableMips()+"  ??????ram"+this.getRam().getAvailableResource()+" host?????????mips???"+this.getVmScheduler().getAllocatedMips(vm));
            vmsMigratingIn.remove(vm);
            vmList.remove(vm);
            return false;
        }

        //???????????????????????????????????????,??????????????????????????????????????????
        this.setCantShutdown(true);
        if(!isActive()){
            System.out.println(getSimulation().clockStr() + ": Host "+ this.getId()+" has been awake for Vm " +vm.getId()+" migration successful!");
            setActive(true);
        }

        ((VmSimple)vm).updateMigrationStartListeners(this);
        updateProcessing(simulation.clock());
        vm.getHost().updateProcessing(simulation.clock());
        return true;
    }

    @Override
    public void removeMigratingInVm(final Vm vm) {
        vmsMigratingIn.remove(vm);
        vmList.remove(vm);
        vm.setInMigration(false);
    }

    @Override
    public Set<Vm> getVmsMigratingOut() {
        return Collections.unmodifiableSet(vmsMigratingOut);
    }

    @Override
    public boolean addVmMigratingOut(final Vm vm) {
        return this.vmsMigratingOut.add(vm);
    }

    @Override
    public boolean removeVmMigratingOut(final Vm vm) {
        return this.vmsMigratingOut.remove(vm);
    }

    @Override
    public Datacenter getDatacenter() {
        return datacenter;
    }

    @Override
    public final void setDatacenter(final Datacenter datacenter) {
        checkSimulationIsRunningAndAttemptedToChangeHost("Datacenter");
        this.datacenter = datacenter;
    }

    @Override
    public String toString() {
        final String dc =
                datacenter == null || Datacenter.NULL.equals(datacenter) ? "" :
                String.format("/DC %d", datacenter.getId());
        return String.format("Host %d%s", getId(), dc);
    }

    @Override
    public boolean removeOnUpdateProcessingListener(final EventListener<HostUpdatesVmsProcessingEventInfo> listener) {
        return onUpdateProcessingListeners.remove(listener);
    }

    @Override
    public Host addOnUpdateProcessingListener(final EventListener<HostUpdatesVmsProcessingEventInfo> listener) {
        if(EventListener.NULL.equals(listener)){
            return this;
        }

        this.onUpdateProcessingListeners.add(requireNonNull(listener));
        return this;
    }

    @Override
    public long getAvailableStorage() {
        return disk.getAvailableResource();
    }

    @Override
    public int getFreePesNumber() {
        return freePesNumber;
    }

    @Override
    public int getWorkingPesNumber() {
        return peList.size() - getFailedPesNumber();
    }

    @Override
    public int getBusyPesNumber() {
        return getWorkingPesNumber() - getFreePesNumber();
    }

    @Override
    public double getBusyPesPercent() {
        return getBusyPesNumber() / (double)getNumberOfPes();
    }

    @Override
    public double getBusyPesPercent(final boolean hundredScale) {
        final double scale = hundredScale ? 100 : 1;
        return getBusyPesPercent() * scale;
    }

    @Override
    public int getFailedPesNumber() {
        return failedPesNumber;
    }

    @Override
    public Simulation getSimulation() {
        return this.simulation;
    }

    @Override
    public double getLastBusyTime() {
        return lastBusyTime;
    }

    @Override
    public final Host setSimulation(final Simulation simulation) {
        this.simulation = simulation;
        return this;
    }

    /**
     * Compare this Host with another one based on {@link #getTotalMipsCapacity()}.
     *
     * @param o the Host to compare to
     * @return {@inheritDoc}
     */
    @Override
    public int compareTo(final Host o) {
        return Double.compare(getTotalMipsCapacity(), o.getTotalMipsCapacity());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final HostSimple that = (HostSimple) o;

        if (id != that.id) return false;
        return simulation.equals(that.simulation);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(id);
        result = 31 * result + simulation.hashCode();
        return result;
    }

    @Override
    public List<ResourceManageable> getResources() {
        if(simulation.isRunning() && resources.isEmpty()){
            resources = Arrays.asList(ramProvisioner.getResource(), bwProvisioner.getResource());
        }

        return Collections.unmodifiableList(resources);
    }

    @Override
    public ResourceProvisioner getProvisioner(final Class<? extends ResourceManageable> resourceClass) {
        if(simulation.isRunning() && provisioners.isEmpty()){
            provisioners = Arrays.asList(ramProvisioner, bwProvisioner);
        }

        return provisioners
            .stream()
            .filter(provisioner -> provisioner.getResource().isSubClassOf(resourceClass))
            .findFirst()
            .orElse(ResourceProvisioner.NULL);
    }

    @Override
    public List<Pe> getWorkingPeList() {
        return getFilteredPeList(Pe::isWorking);
    }

    @Override
    public List<Pe> getBusyPeList() {
        return getFilteredPeList(Pe::isBusy);
    }

    @Override
    public List<Pe> getFreePeList() {
        return getFilteredPeList(Pe::isFree);
    }

    private List<Pe> getFilteredPeList(final Predicate<Pe> status) {
        return peList.stream().filter(status).collect(toList());
    }

    @Override
    public double getCpuPercentUtilization() {
        return computeCpuUtilizationPercent(getCpuMipsUtilization());
    }

    private double computeCpuUtilizationPercent(final double mipsUsage){
        final double totalMips = getTotalMipsCapacity();
        if(totalMips == 0){
            return 0;
        }

        final double utilization = mipsUsage / totalMips;
        return utilization;
//        return (utilization > 1 && utilization < 1.01 ? 1 : utilization);
    }

    @Override
    public double getCpuMipsUtilization() {
        return vmList.stream().mapToDouble(Vm::getTotalCpuMipsUtilization).sum();
    }

    @Override
    public long getRamUtilization() {
        return vmList.stream().mapToLong(Vm::getCurrentRequestedRam).sum();
    }

    @Override
    public double getRamPercentUtilization() {
        return computeRamUtilizationPercent(getRamUtilization());
    }

    private double computeRamUtilizationPercent(final long ramUsage){
        final double totalRam = ram.getCapacity();
        if(totalRam == 0){
            return 0;
        }
        final double utilization = ramUsage/ totalRam;
        return (utilization > 1 && utilization < 1.01 ? 1 : utilization);
    }

    @Override
    public long getBwUtilization() {
        return bwProvisioner.getTotalAllocatedResource();
    }

    @Override
    public HostResourceStats getCpuUtilizationStats() {
        return cpuUtilizationStats;
    }


    @Override
    public void enableUtilizationStats() {
        if (cpuUtilizationStats != null && cpuUtilizationStats != HostResourceStats.NULL) {
            return;
        }

        this.cpuUtilizationStats = new HostResourceStats(this, Host::getCpuPercentUtilization);
        if(vmList.isEmpty()){
            final String host = this.getId() > -1 ? this.toString() : "Host";
            LOGGER.info("Automatically enabling computation of utilization statistics for VMs on {} could not be performed because it doesn't have VMs yet. You need to enable it for each VM created.", host);
        }
        else vmList.forEach(ResourceStatsComputer::enableUtilizationStats);
    }

    @Override
    public PowerModelHost getPowerModel() {
        return powerModel;
    }

    @Override
    public final void setPowerModel(final PowerModelHost powerModel) {
        Objects.requireNonNull(powerModel,
            "powerModel cannot be null. You could provide a " +
            PowerModelHost.class.getSimpleName() + ".NULL instead.");

        if(powerModel.getHost() != null && powerModel.getHost() != Host.NULL && !this.equals(powerModel.getHost())){
            throw new IllegalStateException("The given PowerModel is already assigned to another Host. Each Host must have its own PowerModel instance.");
        }

        this.powerModel = powerModel;
        powerModel.setHost(this);
    }

    @Override
    public void enableStateHistory() {
        this.stateHistoryEnabled = true;
    }

    @Override
    public void disableStateHistory() {
        this.stateHistoryEnabled = false;
    }

    @Override
    public boolean isStateHistoryEnabled() {
        return this.stateHistoryEnabled;
    }

    @Override
    public List<Vm> getFinishedVms() {
        return getVmList().stream()
            .filter(vm -> !vm.isInMigration())
            .filter(vm -> vm.getCurrentRequestedTotalMips() == 0)
            .collect(toList());
    }

    /**
     * Adds the VM resource usage to the History if the VM is not migrating into the Host.
     * @param vm the VM to add its usage to the history
     * @param currentTime the current simulation time
     * @return the total allocated MIPS for the given VM
     */
    private double addVmResourceUseToHistoryIfNotMigratingIn(final Vm vm, final double currentTime) {
        double totalAllocatedMips = getVmScheduler().getTotalAllocatedMipsForVm(vm);
        if (getVmsMigratingIn().contains(vm)) {
            LOGGER.info("{}: {}: {} is migrating in", getSimulation().clockStr(), this, vm);
            return totalAllocatedMips;
        }
        final double totalRequestedMips = vm.getCurrentUtilizationTotalMips();
        if (totalAllocatedMips + 0.1 < totalRequestedMips) {
            BigDecimal b1 = new BigDecimal(Double.toString(totalAllocatedMips));
            BigDecimal b2 = new BigDecimal(Double.toString(totalRequestedMips));
            final String reason = getVmsMigratingOut().contains(vm) ? "migration overhead" : "capacity unavailability";
            final double notAllocatedMipsByPe = (b2.subtract(b1).doubleValue())/vm.getNumberOfPes();
            LOGGER.warn(
                "{}: {}: {} MIPS not allocated for each one of the {} PEs from {} due to {}.",
                getSimulation().clockStr(), this, notAllocatedMipsByPe, vm.getNumberOfPes(), vm, reason);
        }

        final VmStateHistoryEntry entry = new VmStateHistoryEntry(
                                                currentTime, totalAllocatedMips, totalRequestedMips,
                                                vm.isInMigration() && !getVmsMigratingIn().contains(vm));
        vm.addStateHistoryEntry(entry);

        if (vm.isInMigration()) {
            LOGGER.info("{}: {}: {} is migrating out ", getSimulation().clockStr(), this, vm);
            totalAllocatedMips /= getVmScheduler().getMaxCpuUsagePercentDuringOutMigration();
        }

        return totalAllocatedMips;
    }

    private void addStateHistory(final double currentTime) {
        if(!stateHistoryEnabled){
            return;
        }

        double hostTotalRequestedMips = 0.0;

        for (final Vm vm : getVmList()) {
            final double totalRequestedMips = vm.getCurrentRequestedTotalMips();
            addVmResourceUseToHistoryIfNotMigratingIn(vm, currentTime);
            hostTotalRequestedMips += totalRequestedMips;
        }
        addStateHistoryEntry(currentTime, getCpuMipsUtilization(),hostTotalRequestedMips,active);
    }

    /**
     * Adds a host state history entry.
     *
     * @param time the time
     * @param allocatedMips the allocated mips
     * @param requestedMips the requested mips
     * @param isActive the is active
     */
    private void addStateHistoryEntry(
        final double time,
        final double allocatedMips,
        final double requestedMips,
        final boolean isActive)
    {
        final HostStateHistoryEntry newState = new HostStateHistoryEntry(time, allocatedMips,requestedMips,isActive);
        if (!stateHistory.isEmpty()) {
            final HostStateHistoryEntry previousState = stateHistory.get(stateHistory.size() - 1);
            if (previousState.getTime() == time) {
                stateHistory.set(stateHistory.size() - 1, newState);
                return;
            }
        }

        stateHistory.add(newState);
    }

    private void addStateHistoryEntry(
        final double time,
        final double allocatedMips,
        final double requestedMips,
        final long allocatedram,
        final long requestram,
        final boolean isActive)
    {
        final HostStateHistoryEntry newState = new HostStateHistoryEntry(time, allocatedMips,requestedMips,allocatedram,requestram,isActive);
        if (!stateHistory.isEmpty()) {
            final HostStateHistoryEntry previousState = stateHistory.get(stateHistory.size() - 1);
            if (previousState.getTime() == time) {
                stateHistory.set(stateHistory.size() - 1, newState);
                return;
            }
        }

        stateHistory.add(newState);
    }

    @Override
    public List<HostStateHistoryEntry> getStateHistory() {
        return Collections.unmodifiableList(stateHistory);
    }

    @Override
    public List<Vm> getMigratableVms() {
        return vmList.stream()
            .filter(vm -> !vm.isInMigration())
            .collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     * <p><b>It's enabled by default.</b></p>
     * @return {@inheritDoc}
     */
    @Override
    public boolean isLazySuitabilityEvaluation() {
        return lazySuitabilityEvaluation;
    }

    @Override
    public Host setLazySuitabilityEvaluation(final boolean lazySuitabilityEvaluation) {
        this.lazySuitabilityEvaluation = lazySuitabilityEvaluation;
        return this;
    }

    public double resourceWastage(){
        double xita = 0.0001;
        double hostCpuCapacity = this.getTotalMipsCapacity();
        double hostRamCapacity = this.getRam().getCapacity();
        double hostCpuUtilization = Math.min(this.getCpuPercentUtilization(),1.0);
        double hostRamUtilization = Math.min(this.getRamPercentUtilization(),1.0);
        if(hostCpuUtilization == 0.0 || hostRamUtilization == 0.0) return 1.0;
        double hostRemindingCpuUtilization = (hostCpuCapacity - hostCpuUtilization * hostCpuCapacity ) / hostCpuCapacity;
        double hostRemindingRamUtilization = (hostRamCapacity - hostRamUtilization * hostRamCapacity ) / hostRamCapacity;
        double wastage = (Math.abs(hostRemindingCpuUtilization - hostRemindingRamUtilization) + xita) / (hostCpuUtilization + hostRamUtilization);
        return wastage;
    }

    public double avgResourceWastage(){
        return resourceWastage()/getVmList().size();
    }

    public List<Double> getCpuUtilizationHistory() {
        double[] utilizationHistory = new double[logLength];
        double hostMips = getTotalMipsCapacity();
        for(Vm vm:getVmList()){
            if(vm.getId() == -1){
                vm = vm.getTempVm();
            }
            for (int i = 0; i < vm.getUtilizationHistory().size(); i++) {
                utilizationHistory[i] += Math.floor(vm.getUtilizationHistory().get(i) * vm.getMips()) / hostMips;
            }
        }
        utilizationHistory = statictrimZeroTail(utilizationHistory);
        LinkedList<Double> usages = new LinkedList<>();
        for(double num:utilizationHistory){
            usages.addLast(num);
        }
        return usages;
    }

    public List<Double> getRamUtilizationHistory() {
        double[] utilizationHistory = new double[logLength];
        double hostRam = getRam().getCapacity();
        for(Vm vm:getVmList()){
            if(vm.getId() == -1){
                vm = vm.getTempVm();
            }
            for (int i = 0; i < vm.getUtilizationHistoryRam().size(); i++) {
                utilizationHistory[i] += Math.floor(vm.getUtilizationHistoryRam().get(i) * vm.getRam().getCapacity()) / hostRam;
            }
        }
        utilizationHistory = statictrimZeroTail(utilizationHistory);
        List<Double> usages = new LinkedList<>();
        for(double num:utilizationHistory){
            usages.add(num);
        }
        return usages;
    }

    public static double[] statictrimZeroTail(final double[] data) {
        return Arrays.copyOfRange(data, 0, staticcountNonZeroBeginning(data));
    }

    public static int staticcountNonZeroBeginning(final double[] data) {
        int i = data.length - 1;
        while (i >= 0) {
            if (data[i--] != 0) {
                break;
            }
        }
        return i + 2;
    }

}
