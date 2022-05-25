package org.cloudbus.cloudsim.selectionpolicies;

import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.vms.Vm;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

public class VmSelectionPolicyMinimumLoad implements VmSelectionPolicy{
    @Override
    public Vm getVmToMigrate(final Host host) {
        final List<? extends Vm> migratableVms = host.getMigratableVms();
        if (migratableVms.isEmpty()) {
            return Vm.NULL;
        }

        final Predicate<Vm> inMigration = Vm::isInMigration;
        final Comparator<? super Vm> cpuUsageComparator =
            Comparator.comparingDouble(vm -> vm.getCpuPercentUtilization(vm.getSimulation().clock()) + (double)vm.getCurrentRequestedRam()/vm.getRam().getCapacity());
        final Optional<? extends Vm> optional = migratableVms.stream()
            .filter(inMigration.negate())
            .min(cpuUsageComparator);
        return optional.isPresent() ? optional.get() : Vm.NULL;
    }
}
