package org.cloudsimplus.MyExample.modifyMigration;

import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigration;
import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigrationAbstract;
import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigrationDynamicUpperThreshold;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.selectionpolicies.VmSelectionPolicy;

public class VmAllocationPolicyPowerAwereMigrationBestFitDynamicUpperThreshold extends VmAllocationPolicyMigrationAbstract
    implements VmAllocationPolicyMigrationDynamicUpperThreshold {
    public VmAllocationPolicyPowerAwereMigrationBestFitDynamicUpperThreshold(VmSelectionPolicy vmSelectionPolicy) {
        super(vmSelectionPolicy);
    }

    @Override
    public double getOverUtilizationThreshold(Host host) {
        return 0;
    }

    @Override
    public double getRamOverUtilizationThreshold(Host host) {
        return 0;
    }

    @Override
    public void setFallbackVmAllocationPolicy(VmAllocationPolicyMigration fallbackPolicy) {

    }

    @Override
    public VmAllocationPolicyMigration getFallbackVmAllocationPolicy() {
        return null;
    }

    @Override
    public double getSafetyParameter() {
        return 0;
    }

    @Override
    public double computeHostUtilizationMeasure(Host host) throws IllegalStateException {
        return 0;
    }
}
