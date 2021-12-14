package org.cloudsimplus.MyExample;

import java.util.*;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.cloudsimplus.MyExample.ARIMA.ARIMAModel;

public class MathHandler {

    public MathHandler() {}

    public static void main(String[] args) {
        MathHandler mathHandler = new MathHandler();
        List<Double> list = new ArrayList<>();
        list.add(0.07947);
        list.add(0.1262);
        list.add(0.1411);
        list.add(0.1409);
        list.add(0.1218);
        list.add(0.1311);
        list.add(0.1338);
        list.add(0.135);
        list.add(0.1289);
        list.add(0.1191);
        list.add(0.1187);
        list.add(0.1219);
//        list.add(1.0);
//        list.add(2.1);
//        list.add(3.3);
//        list.add(4.5);
//        list.add(5.2);
//        list.add(6.34);
//        list.add(7.66);
//        list.add(8.0);
//        list.add(9.3);
//        list.add(10.2);
//        list.add(11.0);
//        list.add(12.3);


//        //暂时还是GM(1,1)靠谱，等待更好的
        mathHandler.GM11PredictingTest(list,list.size());
//        //结论：DGM是个乐色算法
//        mathHandler.DGM21PredictingTest(list,12);
//        mathHandler.DGM11PredictingTest(list,12);
        //ARIMA预测效果更好
        System.out.println("Predict value="+mathHandler.ARIMRPrediction(list,Constant.HOST_LogLength));
    }

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

    public double ARIMRPrediction(List<Double> dataHistory,int n){
        double[] data = convertListToArray(dataHistory);
        ARIMAModel arima = new ARIMAModel(data);
        ArrayList<double []> list = new ArrayList<>();
        int period = 1;
        int modelCnt = 5, cnt = 0;			//通过多次预测的平均值作为预测值
        double[] tmpPredict = new double[modelCnt];
        for (int k = 0; k < modelCnt; ++k)			//控制通过多少组参数进行计算最终的结果
        {
            double [] bestModel = arima.getARIMAModel(period, list, k != 0);
            if (bestModel.length == 0)
            {
                tmpPredict[k] = data[data.length - period];
                cnt++;
                break;
            }
            else
            {
                double predictDiff = arima.predictValue((int)bestModel[0], (int)bestModel[1], period);
                tmpPredict[k] = arima.aftDeal(predictDiff, period);
                cnt++;
            }
//            System.out.println("BestModel is " + bestModel[0] + " " + bestModel[1]);
            list.add(bestModel);
        }
        double sumPredict = 0.0;
        for (int k = 0; k < cnt; ++k)
        {
//            System.out.println(tmpPredict[k]);
            sumPredict += tmpPredict[k] / (double)cnt;
        }
        //        System.out.println("Predict value="+predict);
        return sumPredict;
    }

    public double DGM21Predicting(List<Double> dataHistory,int n,double utilization,boolean max){
        double[] originalSequence = listToArray(dataHistory,n);
        //若历史记录不满足log长度，无法预测，直接返回当前利用率当作预测值
        if(dataHistory.size() < n || checkUtilizationZero(originalSequence)){
            return utilization;
        }
        int tn = n-1;
        double[] cumulativeSequence = calculateCumulativeSequence(originalSequence,n);
        double[] IAGO = reverseCalculateCumulativeSequence(originalSequence,tn);
//        double[] meanSequence = calculateMeanSequence(cumulativeSequence,tn);
        double[][] B = new double[tn][2];
        double[] temp = new double[tn];
        for(int i=0;i<tn;++i){
            temp[i] = originalSequence[i+1];
        }
        initialB(B,Arrays.copyOfRange(originalSequence, 1, n+1),tn);
        double[][] YN = new double[tn][1];
        initialDGMYN(YN,IAGO,tn);
        //DGM和GM在这里都是一样的矩阵乘法
        double[][] result = calculateGM11AandB(B,YN);
        double a = result[0][0],b = result[1][0];
//        double predict = getGM11PredictResult(a,b,n+1,originalSequence);
        double[] predicts = getKDGM21PredictResult(a,b,n,originalSequence);
        if(max){
            return cutTo0To1(findPredictMax(predicts));
        }else{
            return cutTo0To1(findPredictMin(predicts));
        }
    }

    public double findMaxValue(double predict,double[] originalSequence){
        double max = 0.0;
        for(double num:originalSequence){
            max = Math.max(num,max);
        }
        return Math.max(max,predict);
    }

    public double findMinValue(double predict,double[] originalSequence){
        double min = 0.0;
        for(double num:originalSequence){
            min = Math.min(num,min);
        }
        return Math.min(min,predict);
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

    public double[] reverseCalculateCumulativeSequence(double[] originalSequence,int n){
        double[] reverseCalculateCumulativeSequence = new double[n-1];
        for(int i=1;i<n;++i){
            reverseCalculateCumulativeSequence[i-1]  = originalSequence[i] - originalSequence[i-1];
        }
        return reverseCalculateCumulativeSequence;
    }

    public double[] listToArray(List<Double> dataHistory,int n){
        double[] originalSequence = new double[dataHistory.size()];
        int i=0;
        for(double utilization:dataHistory){
            originalSequence[i++] = utilization;
        }
        return originalSequence;
    }

    public double GM11Predicting(List<Double> dataHistory,int n,double utilization,boolean max){
        if(!Constant.USING_PREDICT){
            return utilization;
        }
        double[] originalSequence = listToArray(dataHistory,n);
        //若历史记录不满足log长度，无法预测，直接返回当前利用率当作预测值
        if(dataHistory.size() < n || checkUtilizationZero(originalSequence)){
            return utilization;
        }

//        for(double num : originalSequence) System.out.println(num);

        int tn = n-1;
        double[] cumulativeSequence = calculateCumulativeSequence(originalSequence,n);
        double[] meanSequence = calculateMeanSequence(cumulativeSequence,tn);
        double[][] B = new double[tn][2];
        initialB(B,meanSequence,tn);
        double[][] YN = new double[tn][1];
        initialYN(YN,originalSequence,tn);
        double[][] result = calculateGM11AandB(B,YN);
        double a = result[0][0],b = result[1][0];
//        double predict = getGM11PredictResult(a,b,n+1,originalSequence);
        double[] predicts = getKGM11PredictResult(a,b,n,originalSequence);
        if(max){
            return cutTo0To1(findPredictMax(predicts));
        }else{
            return cutTo0To1(findPredictMin(predicts));
        }
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

    public void initialDGMYN(double[][] YN,double[] IAGO,int tn){
        for(int i=0;i<tn;++i){
            for(int j=0;j<1;++j){
                YN[i][j] = IAGO[i];
            }
        }
    }

    public void initialDGM11YN(double[][] YN,double[] array,int tn){
        for(int i=0;i<tn;++i){
            for(int j=0;j<1;++j){
                YN[i][j] = array[i+1];
            }
        }
    }

    public RealMatrix inverseMatrix(RealMatrix A) {
        return new LUDecomposition(A).getSolver().getInverse();
    }

    public double[][] calculateGM11AandB(double[][] B,double[][] YN){
        //B转为B矩阵
        RealMatrix BMatrix = new Array2DRowRealMatrix(B);
        //YN转为YN矩阵
        RealMatrix YNMatrix = new Array2DRowRealMatrix(YN);
        //B的转置矩阵
        RealMatrix BTMatrix = BMatrix.transpose();
//        System.out.println("BTMatrix:" +BTMatrix);
        //B的转置矩阵*B矩阵
        RealMatrix B2TMatrix = BTMatrix.multiply(BMatrix);
//        System.out.println("B2TMatrix:" +B2TMatrix);
        //B的转置矩阵*B矩阵的逆矩阵
        RealMatrix B_2TMatrix = inverseMatrix(B2TMatrix);
//        System.out.println("B_2TMatrix:" +B_2TMatrix);
        //B的转置矩阵*B矩阵的逆矩阵 * B的转置矩阵
        RealMatrix A = B_2TMatrix.multiply(BTMatrix);
//        System.out.println("A:" +A);
        //B的转置矩阵*B矩阵的逆矩阵 * B的转置矩阵 * YN矩阵
        RealMatrix C = A.multiply(YNMatrix);
//        System.out.println("C:" +C);
        return C.getData();
    }

    public double getGM11PredictResult(double a,double b,int k,double[] originalSequence){
//        return (originalSequence[0]-b/a) * Math.exp(-a * (k-1)) - (originalSequence[0]-b/a) * Math.exp(-a * (k-2));
        return (originalSequence[0]-b/a) * Math.exp(-a * (k-1)) - (originalSequence[0]-b/a) * Math.exp(-a * (k-2));
    }

    public double getDGM21PredictResult(double a,double b,int k,double[] originalSequence){
        return getDGMcumulativePredictValue(a,b,k,originalSequence) - getDGMcumulativePredictValue(a,b,k-1,originalSequence);
    }

    public double getDGM11PredictResult(double a,double b,int k,double[] originalSequence){
        return getDGM11cumulativePredictValue(a,b,k,originalSequence) - getDGMcumulativePredictValue(a,b,k-1,originalSequence);
    }

    public double getDGMcumulativePredictValue(double a,double b,int k,double[] originalSequence){
        return ( b/(a*a) - originalSequence[0]/a ) * Math.exp(-a * (k)) + (b/a) * (k) + originalSequence[0] * ((1+a)/a) - b/(a*a);
//        return (b/(a*a) - originalSequence[0]/a) * Math.exp(-a * (k-1)) + b/a*(k-1) + (1+a)/a*originalSequence[0] - b/(a*a);
    }

    public double getDGM11cumulativePredictValue(double a,double b,int k,double[] originalSequence){
        return Math.pow(a,k-1) * originalSequence[0] + (1-Math.pow(a,k-1))/(1-a) * b;
    }

    //参与预测的利用率中不能有0，否则为奇异矩阵（不满秩），无法计算逆矩阵
    public boolean checkUtilizationZero(double[] originalSequence){
        Set<Double> set = new HashSet<>();
        for(double num:originalSequence){
            if(!set.contains(num)){
                set.add(num);
            }else return true;
        }
        return false;
    }

    double cutTo0To1(double predict){
        return Math.min(1,Math.max(0,predict));
    }

    boolean checkUtilizationsBelowThreadHold(double[] utilizations,double threadhold){
        for(double utilization:utilizations){
            if(utilization > threadhold){
                return false;
            }
        }
        return true;
    }

    boolean checkUtilizationsUpperThreadHold(double[] utilizations,double threadhold){
        for(double utilization:utilizations){
            if(utilization < threadhold){
                return false;
            }
        }
        return true;
    }

    double[] getKGM11PredictResult(double a,double b,int n,double[] originalSequence){
        int K = Constant.KSTEP;
        double[] utilizations = new double[K];
        for(int i=0;i<K;++i){
            utilizations[i] = getGM11PredictResult(a,b,n+i+1,originalSequence);
        }
        return  utilizations;
    }

    double[] getKDGM21PredictResult(double a,double b,int n,double[] originalSequence){
        int K = Constant.KSTEP;
        double[] utilizations = new double[K];
        for(int i=0;i<K;++i){
            utilizations[i] = getDGM21PredictResult(a,b,n+i+1,originalSequence);
        }
        return  utilizations;
    }

    double[] getKDGM11PredictResult(double a,double b,int n,double[] originalSequence){
        int K = Constant.KSTEP;
        double[] utilizations = new double[K];
        for(int i=0;i<K;++i){
            utilizations[i] = getDGM11PredictResult(a,b,n+i+1,originalSequence);
        }
        return  utilizations;
    }

    double findPredictMax(double[] predicts){
        return Arrays.stream(predicts).max().getAsDouble();
    }

    double findPredictMin(double[] predicts){
        return Arrays.stream(predicts).min().getAsDouble();
    }

    public double GM11PredictingTest(List<Double> dataHistory,int n){
        double[] originalSequence = listToArray(dataHistory,n);
        int tn = n-1;
        double[] cumulativeSequence = calculateCumulativeSequence(originalSequence,n);
        double[] meanSequence = calculateMeanSequence(cumulativeSequence,tn);
        double[][] B = new double[tn][2];
        initialB(B,meanSequence,tn);
        double[][] YN = new double[tn][1];
        initialYN(YN,originalSequence,tn);
        double[][] result = calculateGM11AandB(B,YN);
        double a = result[0][0],b = result[1][0];
        double[] predicts = getKGM11PredictResult(a,b,n,originalSequence);
        for(int i=0;i<predicts.length;++i){
            System.out.println("GM(1，1)的第"+i+"个预测结果："+predicts[i]);
        }
        return getGM11PredictResult(a,b,13,originalSequence);
    }

    public double DGM21PredictingTest(List<Double> dataHistory,int n){
        double[] originalSequence = listToArray(dataHistory,n);
//        for(int i=0;i<n;++i){
//            originalSequence[i] *= 10;
//        }
        int tn = n-1;
        double[] cumulativeSequence = calculateCumulativeSequence(originalSequence,n);
        double[] IAGO = reverseCalculateCumulativeSequence(originalSequence,n);
//        double[] meanSequence = calculateMeanSequence(cumulativeSequence,tn);
        double[][] B = new double[tn][2];
        double[] temp = new double[tn];
        for(int i=0;i<tn;++i){
            temp[i] = originalSequence[i+1];
        }
        initialB(B,Arrays.copyOfRange(originalSequence, 1, n),tn);
        double[][] YN = new double[tn][1];
        initialDGMYN(YN,IAGO,tn);
        //DGM和GM在这里都是一样的矩阵乘法
        double[][] result = calculateGM11AandB(B,YN);
        double a = result[0][0],b = result[1][0];
//        double predict = getGM11PredictResult(a,b,n+1,originalSequence);
        double[] predicts = getKDGM21PredictResult(a,b,n,originalSequence);
//        for(int i=0;i<Constant.KSTEP;++i) predicts[i] /= 10;
        for(int i=0;i<predicts.length;++i){
            System.out.println("DGM(2，1)的第"+i+"个预测结果："+predicts[i]);
        }
        return predicts[0];
    }

    public double DGM11PredictingTest(List<Double> dataHistory,int n){
        double[] originalSequence = listToArray(dataHistory,n);
        int tn = n-1;
        double[] cumulativeSequence = calculateCumulativeSequence(originalSequence,n);
        double[][] B = new double[tn][2];
        initialB(B,Arrays.copyOfRange(cumulativeSequence, 0, n),tn);
        double[][] YN = new double[tn][1];
        initialDGM11YN(YN,cumulativeSequence,tn);
        //DGM和GM在这里都是一样的矩阵乘法
        double[][] result = calculateGM11AandB(B,YN);
        double a = result[0][0],b = result[1][0];
        double[] predicts = getKDGM11PredictResult(a,b,n,originalSequence);
        for(int i=0;i<predicts.length;++i){
            System.out.println("DGM(1，1)的第"+i+"个预测结果："+predicts[i]);
        }
        return predicts[0];
    }

    public double mad(List<Double> usages){
        double mad = 0.0;
        if(usages.size() > 0){
            double[] data = convertListToArray(usages);
            double median = getMedian(data);
            double[] deviationSum = new double[data.length];
            for (int i = 0; i < data.length; i++) {
                deviationSum[i] = Math.abs(median - data[i]);
            }
            mad = getMedian(deviationSum);
        }
        return mad;
    }

    public double iqr(List<Double> usages){
        double[] data = convertListToArray(usages);
        Arrays.sort(data);
        int q1 = (int) Math.round(0.25 * (data.length + 1)) - 1;
        int q3 = (int) Math.round(0.75 * (data.length + 1)) - 1;
        return data[q3] - data[q1];
    }

    public double[] convertListToArray(List<Double> list){
        double[] data = new double[list.size()];
        int i=0;
        for(double num:list){
            data[i++] = num;
        }
        return data;
    }

    public double getMedian(double[] data){
        return getStatistics(data).getPercentile(50);
    }

    public DescriptiveStatistics getStatistics(final double[] list) {
        DescriptiveStatistics stats = new DescriptiveStatistics();
        for (double v : list) {
            stats.addValue(v);
        }
        return stats;
    }

    public double[] getLoessParameterEstimates(final double[] y) {
        int n = y.length;
        double[] x = new double[n];
        for (int i = 0; i < n; i++) {
            x[i] = i + 1;
        }
        return createWeigthedLinearRegression(x, y, getTricubeWeigts(n)).regress().getParameterEstimates();
    }

    public double[] getTricubeWeigts(final int n) {
        double[] weights = new double[n];
        double top = n - 1;
        double spread = top;
        for (int i = 2; i < n; i++) {
            double k = Math.pow(1 - Math.pow((top - i) / spread, 3), 3);
            if (k > 0) {
                weights[i] = 1 / k;
            } else {
                weights[i] = Double.MAX_VALUE;
            }
        }
        weights[0] = weights[1] = weights[2];
        return weights;
    }

    public SimpleRegression createWeigthedLinearRegression(
        final double[] x, final double[] y, final double[] weigths) {
        double[] xW = new double[x.length];
        double[] yW = new double[y.length];

        int numZeroWeigths = 0;
        for (double weigth : weigths) {
            if (weigth <= 0) {
                numZeroWeigths++;
            }
        }

        for (int i = 0; i < x.length; i++) {
            if (numZeroWeigths >= 0.4 * weigths.length) {
                // See: http://www.ncsu.edu/crsc/events/ugw07/Presentations/Crooks_Qiao/Crooks_Qiao_Alt_Presentation.pdf
                xW[i] = Math.sqrt(weigths[i]) * x[i];
                yW[i] = Math.sqrt(weigths[i]) * y[i];
            } else {
                xW[i] = x[i];
                yW[i] = y[i];
            }
        }

        return createLinearRegression(xW, yW);
    }

    public SimpleRegression createLinearRegression(final double[] x, final double[] y) {
        SimpleRegression regression = new SimpleRegression();
        for (int i = 0; i < x.length; i++) {
            regression.addData(x[i], y[i]);
        }
        return regression;
    }

}
