package org.cloudsimplus.MyExample;

import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigration;
import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigrationDynamicUpperThresholdFirstFit;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.selectionpolicies.VmSelectionPolicy;

public class myVmAllocationPolicyMigrationModifyThreshold extends VmAllocationPolicyMigrationDynamicUpperThresholdFirstFit {
    public myVmAllocationPolicyMigrationModifyThreshold(VmSelectionPolicy vmSelectionPolicy) {
        super(vmSelectionPolicy);
    }

    public myVmAllocationPolicyMigrationModifyThreshold(VmSelectionPolicy vmSelectionPolicy, double safetyParameter, VmAllocationPolicyMigration fallbackVmAllocationPolicy) {
        super(vmSelectionPolicy, safetyParameter, fallbackVmAllocationPolicy);
    }

    @Override
    public boolean isParallelHostSearchEnabled() {
        return super.isParallelHostSearchEnabled();
    }

    @Override
    public boolean areHostsUnderOrOverloaded() {
        return super.areHostsUnderOrOverloaded();
    }

    @Override
    public double computeHostUtilizationMeasure(Host host) throws IllegalStateException {
        return 0;
    }
}
