package org.cloudbus.cloudsim.selectionpolicies;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.cloudbus.cloudsim.util.MathUtil;

import java.util.LinkedList;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.vms.Vm;

import java.util.List;

public class VmSelectionPolicyMC implements VmSelectionPolicy{
    private VmSelectionPolicy fallbackPolicy;
    public VmSelectionPolicyMC(final VmSelectionPolicy fallbackPolicy) {
        super();
        setFallbackPolicy(fallbackPolicy);
    }

    @Override
    public Vm getVmToMigrate(final Host host) {
        List<Vm> migratableVms = host.getMigratableVms();
        if (migratableVms.isEmpty()) {
            return null;
        }
        List<Double> metrics = null;
        try {
            metrics = getCorrelationCoefficients(getUtilizationMatrix(migratableVms));
        } catch (IllegalArgumentException e) { // the degrees of freedom must be greater than zero
            return getFallbackPolicy().getVmToMigrate(host);
        }
        double maxMetric = Double.MIN_VALUE;
        int maxIndex = 0;
        for (int i = 0; i < metrics.size(); i++) {
            double metric = metrics.get(i);
            if (metric > maxMetric) {
                maxMetric = metric;
                maxIndex = i;
            }
        }
        return migratableVms.get(maxIndex);
    }

    /**
     * Gets the CPU utilization percentage matrix for a given list of VMs.
     *
     * @param vmList the VM list
     * @return the CPU utilization percentage matrix, where each line i
     * is a VM and each column j is a CPU utilization percentage history for that VM.
     */
    protected double[][] getUtilizationMatrix(final List<Vm> vmList) {
        int n = vmList.size();
                /*@todo It gets the min size of the history among all VMs considering
                that different VMs can have different history sizes.
                However, the j loop is not using the m variable
                but the size of the vm list. If a VM list has
                a size greater than m, it will thow an exception.
                It as to be included a test case for that.*/
        int m = getMinUtilizationHistorySize(vmList);
        double[][] utilization = new double[n][m];
        for (int i = 0; i < n; i++) {
            List<Double> vmUtilization = vmList.get(i).getUtilizationHistory();
            for (int j = 0; j < m; j++) {
                utilization[i][j] = vmUtilization.get(j);
            }
        }
        return utilization;
    }

    /**
     * Gets the min CPU utilization percentage history size among a list of VMs.
     *
     * @param vmList the VM list
     * @return the min CPU utilization percentage history size of the VM list
     */
    protected int getMinUtilizationHistorySize(final List<Vm> vmList) {
        int minSize = Integer.MAX_VALUE;
        for (Vm vm : vmList) {
            int size = vm.getUtilizationHistory().size();
            if (size < minSize) {
                minSize = size;
            }
        }
        return minSize;
    }

    /**
     * Gets the correlation coefficients.
     *
     * @param data the data
     * @return the correlation coefficients
     */
    protected List<Double> getCorrelationCoefficients(final double[][] data) {
        int n = data.length;
        int m = data[0].length;
        List<Double> correlationCoefficients = new LinkedList<Double>();
        for (int i = 0; i < n; i++) {
            double[][] x = new double[n - 1][m];
            int k = 0;
            for (int j = 0; j < n; j++) {
                if (j != i) {
                    x[k++] = data[j];
                }
            }

            // Transpose the matrix so that it fits the linear model
            double[][] xT = new Array2DRowRealMatrix(x).transpose().getData();

            // RSquare is the "coefficient of determination"
            correlationCoefficients.add(MathUtil.createLinearRegression(xT,
                data[i]).calculateRSquared());
        }
        return correlationCoefficients;
    }

    /**
     * Gets the fallback policy.
     *
     * @return the fallback policy
     */
    public VmSelectionPolicy getFallbackPolicy() {
        return fallbackPolicy;
    }

    /**
     * Sets the fallback policy.
     *
     * @param fallbackPolicy the new fallback policy
     */
    public void setFallbackPolicy(final VmSelectionPolicy fallbackPolicy) {
        this.fallbackPolicy = fallbackPolicy;
    }
}
