package org.cloudsimplus.MyExample.modifyMigration;

import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicyAbstract;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.vms.Vm;

import java.util.List;
import java.util.Optional;

public class VmAllocationPolicyNPA extends VmAllocationPolicyAbstract {
    /** @see #getLastHostIndex() */
    private int lastHostIndex;

    @Override
    protected Optional<Host> defaultFindHostForVm(final Vm vm) {
        final List<Host> hostList = getHostList();
        /* The for loop just defines the maximum number of Hosts to try.
         * When a suitable Host is found, the method returns immediately. */
        final int maxTries = hostList.size();
        for (int i = 0; i < maxTries; i++) {
            final Host host = hostList.get(lastHostIndex);
            if (host.isSuitableForVm(vm)) {
                return Optional.of(host);
            }

            /* If it gets here, the previous Host doesn't have capacity to place the VM.
             * Then, moves to the next Host.*/
            incLastHostIndex();
        }

        return Optional.empty();
    }

    /**
     * Gets the index of the last host where a VM was placed.
     */
    protected int getLastHostIndex() {
        return lastHostIndex;
    }

    /**
     * Increment the index to move to the next Host.
     * If the end of the Host list is reached, starts from the beginning. */
    protected void incLastHostIndex() {
        lastHostIndex = ++lastHostIndex % getHostList().size();
    }
}
