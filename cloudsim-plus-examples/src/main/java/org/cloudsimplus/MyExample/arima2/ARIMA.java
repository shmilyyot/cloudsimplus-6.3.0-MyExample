package org.cloudsimplus.MyExample.arima2;

import java.util.ArrayList;
import java.util.Random;
import java.util.Vector;

public class ARIMA {
    double[] originalData;
    double[] dataFirDiff = {};

    Vector<double[]> arimaCoe = new Vector<>();

    public ARIMA(double[] originalData) {
        this.originalData = originalData;
    }

    public double[] preFirDiff(double[] preData){      //一阶差分(1)
        double[] tmpData = new double[preData.length - 1];
        for (int i = 0; i < preData.length - 1; ++i) {
            tmpData[i] = preData[i + 1] - preData[i];
        }
        return tmpData;
    }

    public double[] preSeasonDiff(double[] preData)       //季节性差分(6, 7)
    {
        double[] tmpData = new double[preData.length - 7];
        for (int i = 0; i < preData.length - 7; ++i) {
            tmpData[i] = preData[i + 7] - preData[i];
        }
        return tmpData;
    }

    public double[] preDealDiff(int period) {
        if (period >= originalData.length - 1)      // 将6也归为季节性差分
        {
            period = 0;
        }
        switch (period) {
            case 0:
                return this.originalData;
            case 1:
                this.dataFirDiff = this.preFirDiff(this.originalData);
                return this.dataFirDiff;
            default:
                return preSeasonDiff(originalData);
        }
    }

    public double[] getARIMAModel(int period, ArrayList<double[]> notModel, boolean needNot) {
        double[] data = this.preDealDiff(period);

        double minAIC = Double.MAX_VALUE;
        double[] bestModel = new double[3];
        int type;
        Vector<double[]> coe;

        // model产生, 即产生相应的p, q参数
        int len = data.length;
        if (len > 5) {
            len = 5;
        }
        int size = ((len + 2) * (len + 1)) / 2 - 1;
        int[][] model = new int[size][2];
        int cnt = 0;
        for (int i = 0; i <= len; ++i) {
            for (int j = 0; j <= len - i; ++j) {
                if (i == 0 && j == 0) continue;
                model[cnt][0] = i;
                model[cnt++][1] = j;
            }
        }
        for(int i=0;i<model.length;i++){
            for(int j=0;j<model[0].length;j++){
                System.out.println("model["+i+"]["+j+"] = "+model[i][j]);
            }
        }

        for (int[] ints : model) {
            // 控制选择的参数
            boolean token = false;
            if (needNot) {
                for (double[] value : notModel) {
                    if (ints[0] == value[0] && ints[1] == value[1]) {
                        token = true;
                        break;
                    }
                }
            }
            if (token) {
                continue;
            }
            if (ints[0] == 0) {
                MA ma = new MA(data, ints[1]);
                coe = ma.solveCoeOfMA();
                type = 1;
            } else if (ints[1] == 0) {
                AR ar = new AR(data, ints[0]);
                coe = ar.solveCoeOfAR();
                type = 2;
            } else {
                ARMA arma = new ARMA(data, ints[0], ints[1]);
                coe = arma.solveCoeOfARMA();
                type = 3;
            }
            // 在求解过程中如果阶数选取过长，可能会出现NAN或者无穷大的情况
            double aic = new ARMAMethod().getModelAIC(coe, data, type);
            if (Double.isFinite(aic) && !Double.isNaN(aic) && aic < minAIC) {
                minAIC = aic;
                bestModel[0] = ints[0];
                bestModel[1] = ints[1];
                bestModel[2] = (int) Math.round(minAIC);
                this.arimaCoe = coe;
            }
        }
        return bestModel;
    }

    public double aftDeal(double predictValue, int period) {
        if (period >= originalData.length) {
            period = 0;
        }

        switch (period) {
            case 0:
                return predictValue;
            case 1:
                return (predictValue + originalData[originalData.length - 1]);
            default:
                return (predictValue + originalData[originalData.length - 7]);
        }
    }

    public double predictValue(double p, double q, int period) {
        double[] data = this.preDealDiff(period);
        int n = data.length;
        double predict;
        double tmpAR = 0.0, tmpMA = 0.0;
        double[] errData = new double[(int)(q + 1)];

        Random random = new Random();

        if (p == 0) {
            double[] maCoe = this.arimaCoe.get(0);
            for (int k = (int)(q); k < n; ++k) {
                tmpMA = 0;
                for (int i = 1; i <= q; ++i) {
                    tmpMA += maCoe[i] * errData[i];
                }
                //产生各个时刻的噪声
                for (int j = (int)q; j > 0; --j) {
                    errData[j] = errData[j - 1];
                }
                errData[0] = random.nextGaussian() * Math.sqrt(maCoe[0]);
            }

            predict = tmpMA; //产生预测
        } else if (q == 0) {
            double[] arCoe = this.arimaCoe.get(0);

            for (int k = (int)p; k < n; ++k) {
                tmpAR = 0;
                for (int i = 0; i < p; ++i) {
                    tmpAR += arCoe[i] * data[k - i - 1];
                }
            }
            predict = tmpAR;
        } else {
            double[] arCoe = this.arimaCoe.get(0);
            double[] maCoe = this.arimaCoe.get(1);

            for (int k = (int)p; k < n; ++k) {
                tmpAR = 0;
                tmpMA = 0;
                for (int i = 0; i < p; ++i) {
                    tmpAR += arCoe[i] * data[k - i - 1];
                }
                for (int i = 1; i <= q; ++i) {
                    tmpMA += maCoe[i] * errData[i];
                }

                //产生各个时刻的噪声
                for (int j = (int)q; j > 0; --j) {
                    errData[j] = errData[j - 1];
                }

                errData[0] = random.nextGaussian() * Math.sqrt(maCoe[0]);
            }

            predict = tmpAR + tmpMA;
        }

        return predict;
    }
}
