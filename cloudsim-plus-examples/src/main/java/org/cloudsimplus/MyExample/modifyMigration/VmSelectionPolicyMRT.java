package org.cloudsimplus.MyExample.modifyMigration;

import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.selectionpolicies.VmSelectionPolicy;
import org.cloudbus.cloudsim.vms.Vm;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

public class VmSelectionPolicyMRT implements VmSelectionPolicy {
    @Override
    public Vm getVmToMigrate(final Host host) {
        final List<? extends Vm> migratableVms = host.getMigratableVms();
        if (migratableVms.isEmpty()) {
            return Vm.NULL;
        }

        final Predicate<Vm> inMigration = Vm::isInMigration;
        final Comparator<? super Vm> newComparator =
            Comparator.comparingDouble(vm -> {
                double cpuusage = host.getCpuPercentUtilization();
                double ramusage = host.getRamPercentUtilization();
                double usage, threshold;
                if(cpuusage > ramusage) {
                    usage = cpuusage - vm.getCpuPercentUtilization() * vm.getMips() / host.getTotalMipsCapacity();
                    threshold = host.getCPU_THRESHOLD();
                }
                else{
                    usage = ramusage - (double)vm.getCurrentRequestedRam() / host.getRam().getCapacity();
                    threshold = host.getRAM_THRESHOLD();
                }
                return Math.pow(usage - threshold, 2) / (double)vm.getCurrentRequestedRam();
            });
        final Optional<? extends Vm> optional = migratableVms.stream()
            .filter(inMigration.negate())
            .max(newComparator);
        return optional.isPresent() ? optional.get() : Vm.NULL;
    }
}
