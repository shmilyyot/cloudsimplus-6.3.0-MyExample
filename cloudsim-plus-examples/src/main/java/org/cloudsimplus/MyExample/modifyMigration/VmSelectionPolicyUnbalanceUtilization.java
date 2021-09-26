package org.cloudsimplus.MyExample.modifyMigration;

import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.selectionpolicies.VmSelectionPolicy;
import org.cloudbus.cloudsim.vms.Vm;

public class VmSelectionPolicyUnbalanceUtilization implements VmSelectionPolicy {
    @Override
    public Vm getVmToMigrate(Host host) {
        return null;
    }
}
