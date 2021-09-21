package org.cloudsimplus.MyExample;

import java.util.Arrays;
import java.util.List;

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

    public void DGMPredicting(List<Double> dataHistory){

    }

}
