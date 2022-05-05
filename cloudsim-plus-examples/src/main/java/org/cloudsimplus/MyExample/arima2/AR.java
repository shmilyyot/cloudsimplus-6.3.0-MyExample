package org.cloudsimplus.MyExample.arima2;

import java.util.Vector;

public class AR {
    private double[] data;
    private int p;

    public AR(double[] data, int p) {
        this.data = data;
        this.p = p;
    }

    public Vector<double[]> solveCoeOfAR() {
        Vector<double[]> vec = new Vector<>();
        double[] arCoe = new ARMAMethod().computeARCoe(this.data, this.p);

        vec.add(arCoe);

        return vec;
    }
}
