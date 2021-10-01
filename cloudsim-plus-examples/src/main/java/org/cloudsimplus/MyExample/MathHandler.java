package org.cloudsimplus.MyExample;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.RealMatrix;

public class MathHandler {

    public MathHandler() {}

    //余弦相似度（暂时只考虑cpu和mem，所以只有二维）
    public double cosSimilarity(double[] x,double[] y){
        double up = getCosineNumerator(x,y);
        double down = getCosineDenomiator(x,y);
        return up/down;
    }

    public double reverseCosSimilarity(double[] x,double[] y){
        return 1-cosSimilarity(x,y);
    }

    //皮尔森相关系数
    public double pearsonCorrelation(double[] x,double[] y){
        double xMean = getMean(x);
        double yMean = getMean(y);
        double numerator = getPersonNumerator(x,y,xMean,yMean);
        double denomiator = getPersonDenomiator(x,y,xMean,yMean);
        return numerator/denomiator;
    }

    public double getMean(double[] data) {
        return Arrays.stream(data).sum()/ data.length;
    }

    public double getCosineNumerator(double[] x,double[] y){
        double numerator = 0.0;
        for(int i=0;i<x.length;++i){
            numerator += x[i]*y[i];
        }
        return numerator;
    }

    public double getPersonNumerator(double[] x,double[] y,double xMean,double yMean){
        double numerator = 0.0;
        for(int i=0;i<x.length;++i){
            numerator += (x[i]-xMean) * (y[i]-yMean);
        }
        return numerator;
    }

    public double getPersonDenomiator(double[] x,double[] y,double xMean,double yMean){
        double xSum = 0.0;
        for (double v : x) {
            xSum += (v - xMean) * (v - xMean);
        }
        double ySum = 0.0;
        for (double v : y) {
            ySum += (v - yMean) * (v - yMean);
        }
        return Math.sqrt(xSum) * Math.sqrt(ySum);
    }

    public double getCosineDenomiator(double[] x,double[] y){
        return Math.sqrt(getSquareSum(x)) * Math.sqrt(getSquareSum(y));
    }

    public double getSquareSum(double[] data){
        double sum = 0.0;
        for (double v : data) {
            sum += Math.pow(v, 2);
        }
        return sum;
    }

    public void ARIMRPrediction(List<Double> dataHistory){

    }

    public double DGM11Predicting(List<Double> dataHistory,int n,double utilization){
        double[] originalSequence = listToArray(dataHistory,n);
        //若历史记录不满足log长度，无法预测，直接返回当前利用率当作预测值
        if(dataHistory.size() < n){
            return utilization;
        }
        double[] cumulativeSequence = calculateCumulativeSequence(originalSequence,n);
        return 0.0;
    }

    public double findMaxValue(double[] prediction){
        return Arrays.stream(prediction).max().getAsDouble();
    }

    public double findMinValue(double[] prediction){
        return Arrays.stream(prediction).min().getAsDouble();
    }

    public double[] calculateCumulativeSequence(double[] originalSequence,int n){
        double[] cumulativeSequence = new double[n];
        cumulativeSequence[0] = originalSequence[0];
        for(int i=1;i<n;++i){
            cumulativeSequence[i] = originalSequence[i]+cumulativeSequence[i-1];
        }
        return cumulativeSequence;
    }

    public double[] calculateMeanSequence(double[] cumulativeSequence,int n){
        double[] meanSequence = new double[n];
        for(int i=0;i<n;++i){
            meanSequence[i] = (cumulativeSequence[i] + cumulativeSequence[i+1])/2;
        }
        return meanSequence;
    }

    public double[] listToArray(List<Double> dataHistory,int n){
        double[] originalSequence = new double[n];
        int i=0;
        for(double utilization:dataHistory){
            originalSequence[i++] = utilization;
        }
        return originalSequence;
    }

    public double GM11Predicting(List<Double> dataHistory,int n,double utilization){
        double[] originalSequence = listToArray(dataHistory,n);
        //若历史记录不满足log长度，无法预测，直接返回当前利用率当作预测值
        if(dataHistory.size() < n){
            return utilization;
        }
        int tn = n-1;
        double[] cumulativeSequence = calculateCumulativeSequence(originalSequence,n);
        double[] meanSequence = calculateMeanSequence(cumulativeSequence,tn);
        double[][] B = new double[tn][2];
        initialB(B,meanSequence,tn);
        double[][] YN = new double[tn][1];
        initialYN(YN,originalSequence,tn);
        double[][] result = calculateAandB(B,YN);
        double a = result[0][0],b = result[1][0];
        int k = n+1;
        double predict = (originalSequence[0]-b/a) * Math.exp(-a * (k+1)) - (originalSequence[0]-b/a) * Math.exp(-a * (k));
        return predict;
    }

    public void initialB(double[][] B,double[] meanSequence,int tn){
        for(int i=0;i<tn;++i){
            for(int j=0;j<2;++j){
                if(j == 1){
                    B[i][j] = 1;
                }else{
                    B[i][j] = -meanSequence[i];
                }
            }
        }
    }

    public void initialYN(double[][] YN,double[] originalSequence,int tn){
        for(int i=0;i<tn;++i){
            for(int j=0;j<1;++j){
                YN[i][j] = originalSequence[i+1];
            }
        }
    }

    public RealMatrix inverseMatrix(RealMatrix A) {
        return new LUDecomposition(A).getSolver().getInverse();
    }

    public double[][] calculateAandB(double[][] B,double[][] YN){
        //B转为B矩阵
        RealMatrix BMatrix = new Array2DRowRealMatrix(B);
        //YN转为YN矩阵
        RealMatrix YNMatrix = new Array2DRowRealMatrix(YN);
        //B的转置矩阵
        RealMatrix BTMatrix = BMatrix.transpose();
        //B的转置矩阵*B矩阵
        RealMatrix B2TMatrix = BTMatrix.multiply(BMatrix);
        //B的转置矩阵*B矩阵的逆矩阵
        RealMatrix B_2TMatrix = inverseMatrix(B2TMatrix);
        //B的转置矩阵*B矩阵的逆矩阵 * B的转置矩阵
        RealMatrix A = B_2TMatrix.multiply(BTMatrix);
        //B的转置矩阵*B矩阵的逆矩阵 * B的转置矩阵 * YN矩阵
        RealMatrix C = A.multiply(YNMatrix);
        return C.getData();
    }

}
