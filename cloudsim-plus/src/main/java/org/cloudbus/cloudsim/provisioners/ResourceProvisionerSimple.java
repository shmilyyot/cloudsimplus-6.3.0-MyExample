/*
 * Title:        CloudSim Toolkit
 * Description:  CloudSim (Cloud Simulation) Toolkit for Modeling and Simulation of Clouds
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009-2012, The University of Melbourne, Australia
 */

package org.cloudbus.cloudsim.provisioners;

import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.resources.*;
import org.cloudbus.cloudsim.vms.Vm;

import java.io.Serializable;
import java.util.Objects;

/**
 * A best-effort {@link ResourceProvisioner} policy used by a {@link Host} to provide a resource to VMs:
 * <ul>
 *  <li>if there is available amount of the resource on the host, it provides;</li>
 *  <li>otherwise, it fails.</li>
 * </ul>
 *
 * @author Rodrigo N. Calheiros
 * @author Anton Beloglazov
 * @author Manoel Campos da Silva Filho
 * @since 3.0.4
 */
public class ResourceProvisionerSimple extends ResourceProvisionerAbstract implements Serializable {
    /**
     * Creates a new ResourceProvisionerSimple which the {@link ResourceManageable}
     * it will manage have to be set further.
     *
     * @see #setResource(ResourceManageable)
     */
    public ResourceProvisionerSimple() {
        super(ResourceManageable.NULL);
    }

    /**
     * Creates a ResourceProvisionerSimple for a given {@link ResourceManageable}.
     *
     * @param resource the resource to be managed by the provisioner
     */
    protected ResourceProvisionerSimple(final ResourceManageable resource) {
        super(resource);
    }

    @Override
    public boolean allocateResourceForVm(final Vm vm, final long newTotalVmResourceCapacity) {
        Objects.requireNonNull(vm);

        if (!isSuitableForVm(vm, newTotalVmResourceCapacity)) {
            return false;
        }

        /* Stores the resource allocation before changing the current allocation,
         * this line, this line must be placed here and not at the end
         * where it's in fact used.*/
        final long prevVmResourceAllocation = vm.getResource(getResourceClass()).getAllocatedResource();

        if (getResourceAllocationMap().containsKey(vm)) {
            //De-allocates any amount of the resource assigned to the Vm in order to allocate a new capacity
            deallocateResourceForVm(vm);
        }

        /*
        Pe resources are not stored in the VM resource List.
        Only the provisioner keeps track of Pe allocation for VM.
        This way, if the resource is not found inside the VM
        and it is a Pe, it's OK (as it is expected)
        */

        if(!getResource().isSubClassOf(Pe.class) && !vm.getResource(getResourceClass()).setCapacity(vm.getResource(getResourceClass()).getCapacity())){
            return false;
        }

        //Allocates the requested resource from the physical resource
        getResource().allocateResource(newTotalVmResourceCapacity);
        getResourceAllocationMap().put(vm, newTotalVmResourceCapacity);
        vm.getResource(getResourceClass()).setCapacity(vm.getResource(getResourceClass()).getCapacity());
        vm.getResource(getResourceClass()).setAllocatedResource(newTotalVmResourceCapacity);
//        if(getResource().isSubClassOf(Ram.class)){
//            System.out.println(newTotalVmResourceCapacity+"   "+getResource().getAvailableResource());
//        }
        return true;
    }

    @Override
    public boolean allocateResourceForVm(final Vm vm, final double newTotalVmResource) {
        return allocateResourceForVm(vm, (long)newTotalVmResource);
    }

    @Override
    public boolean deallocateResourceForVm(final Vm vm) {
        final long amountFreed = deallocateResourceForVmAndSetAllocationMapEntryToZero(vm);
        getResourceAllocationMap().remove(vm);
        return amountFreed > 0;
    }

    @Override
    protected long deallocateResourceForVmAndSetAllocationMapEntryToZero(final Vm vm) {
        if (getResourceAllocationMap().containsKey(vm)) {
            final long vmAllocatedResource = getResourceAllocationMap().get(vm);
            getResourceAllocationMap().put(vm, 0L);
            //De-allocates the virtual resource the VM was using
            vm.deallocateResource(getResourceClass());

            //De-allocates the virtual resource from the physical resource
            getResource().deallocateResource(vmAllocatedResource);
            return vmAllocatedResource;
        }

        return 0;
    }

    @Override
    public boolean isSuitableForVm(final Vm vm, final long newVmTotalAllocatedResource) {
        final long currentAllocatedResource = getAllocatedResourceForVm(vm);
        final long allocationDifference = newVmTotalAllocatedResource - currentAllocatedResource;
//        if(getResource().isSubClassOf(Ram.class) && vm.getSimulation().clock() >= 10800.0 &&  vm.getSimulation().clock() <= 10801.0 && (vm.getId() == 1270 || vm.getId() == 1255)){
//            System.out.println(vm + " 请求的ram："+" 其所在host是"+ vm.getHost()+" "+ newVmTotalAllocatedResource+" difference: "+allocationDifference+"  available: "+getResource().getAvailableResource());
//        }
        return getResource().getAvailableResource() >=  allocationDifference;
    }

    @Override
    public boolean isSuitableForVm(final Vm vm, final Resource resource) {
        if(getResource().isSubClassOf(Ram.class) && vm.getId() != -1){
            return isSuitableForVm(vm, vm.getCurrentRequestedRam());
        }else if(getResource().isSubClassOf(Bandwidth.class) && vm.getId() != -1){
            return isSuitableForVm(vm, vm.getCurrentRequestedBw());
        }
        return isSuitableForVm(vm, resource.getCapacity());
    }
}
