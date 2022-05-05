package org.cloudsimplus.MyExample;

import java.io.*;
import java.util.*;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudsimplus.MyExample.ARIMA.ARIMAModel;
import org.cloudsimplus.MyExample.arima2.ARIMA;

public class MathHandler {

    public MathHandler() {}

    public static void main(String[] args) throws IOException {
        MathHandler mathHandler = new MathHandler();
        List<Double> list = new LinkedList<>();
//        list.add(0.07947);
//        list.add(0.1262);
//        list.add(0.1411);
//        list.add(0.1409);
//        list.add(0.1218);
//        list.add(0.1311);
//        list.add(0.1338);
//        list.add(0.135);
//        list.add(0.1289);
//        list.add(0.1191);
//        list.add(0.1187);
//        list.add(0.1219);
        list.add(1.0);
        list.add(2.1);
        list.add(3.3);
        list.add(4.5);
        list.add(5.2);
        list.add(6.34);
        list.add(7.66);
        list.add(8.0);
        list.add(9.3);
        list.add(10.2);
        list.add(11.0);
        list.add(12.3);
//        list.add(0.012800751879699247);
//        list.add(0.012020676691729326);
//        list.add(0.01954887218045113);
//        list.add(0.02405075187969925);
//        list.add(0.024182330827067666);
//        list.add(0.013167293233082706);
//        list.add(0.015714285714285712);
//        list.add(0.014454887218045111);
//        list.add(0.022781954887218046);
//        list.add(0.02450187969924812);
//        list.add(0.027077067669172936);
//        list.add(0.012988721804511277);
//        Collections.reverse(list);

//        System.out.println(mathHandler.DGM21PredictingTest(list,list.size()));
//        //暂时还是GM(1,1)靠谱，等待更好的
//        System.out.println(mathHandler.GM11PredictingTest(list,list.size()));
//        //结论：DGM是个乐色算法
//        mathHandler.DGM21PredictingTest(list,12);
//        mathHandler.DGM11PredictingTest(list,12);
        //ARIMA预测效果更好
//        System.out.println("Predict value="+mathHandler.ARIMRPrediction(list,list.size()));
//        System.out.println(mathHandler.LRPredicting(list,list.size()));
        mathHandler.handlePredictValue(mathHandler);

    }

    public void handlePredictValue(MathHandler mathHandler) throws IOException {
        ArrayList<Double> originalData = new ArrayList<>();
        ArrayList<Double> predictData = new ArrayList<>();
        ArrayList<Double> originalDataram = new ArrayList<>();
        ArrayList<Double> predictDataram = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader("D:\\java_workspace\\cloudsimplus-6.3.0-MyExample\\cloudsim-plus-examples\\src\\main\\java\\org\\cloudsimplus\\MyExample\\logs\\originalcpu3.csv"));
        String line = null;
        while((line = br.readLine()) != null)
        {
            Double num = Double.parseDouble(line);
            originalData.add(num);
        }
        ArrayList<Double> currentList = new ArrayList<>();
        for(int i = 0; i < 12; ++i)
        {
            currentList.add(originalData.get(i));
        }
        double predictnum = mathHandler.ARIMRPrediction(currentList,Constant.HOST_LogLength);
//        double predictnum = mathHandler.LRPredicting(currentList,Constant.HOST_LogLength);
//        double predictnum = mathHandler.GM11PredictingTest(currentList,Constant.HOST_LogLength);
        predictData.add(predictnum);
        System.out.println("ori: "+ originalData.get(12) + "pre: " + predictnum);
        for(int i = 12; i < originalData.size(); ++i)
        {
            currentList.remove(0);
            currentList.add(originalData.get(i));
            predictnum = mathHandler.ARIMRPrediction(currentList,Constant.HOST_LogLength);
//            predictnum = mathHandler.LRPredicting(currentList,Constant.HOST_LogLength);
//            predictnum = mathHandler.GM11PredictingTest(currentList,Constant.HOST_LogLength);
            if(i+1 < originalData.size())
            {
                System.out.println("ori: "+ originalData.get(i+1) + "pre: " + predictnum);
            }
            else
            {
                System.out.println("pre: " + predictnum);
            }
            predictData.add(predictnum);
        }

        System.out.println("----------------------------------------------------------------------------------");

        br = new BufferedReader(new FileReader("D:\\java_workspace\\cloudsimplus-6.3.0-MyExample\\cloudsim-plus-examples\\src\\main\\java\\org\\cloudsimplus\\MyExample\\logs\\originalram3.csv"));

        String line2 = null;
        while((line2 = br.readLine()) != null)
        {
            Double num = Double.parseDouble(line2);
            originalDataram.add(num);
        }
        ArrayList<Double> currentList2 = new ArrayList<>();
        for(int i = 0; i < 12; ++i)
        {
            currentList2.add(originalDataram.get(i));
        }
        double predictnum2 = mathHandler.ARIMRPrediction(currentList2,Constant.HOST_LogLength);
//        double predictnum2 = mathHandler.LRPredicting(currentList2,Constant.HOST_LogLength);
//        double predictnum2 = mathHandler.GM11PredictingTest(currentList2,Constant.HOST_LogLength);
        predictDataram.add(predictnum2);
        System.out.println("ori: "+ originalDataram.get(12) + "pre: " + predictnum2);
        for(int i = 12; i < originalDataram.size(); ++i)
        {
            currentList2.remove(0);
            currentList2.add(originalDataram.get(i));
            predictnum2 = mathHandler.ARIMRPrediction(currentList2,Constant.HOST_LogLength);
//            predictnum2 = mathHandler.LRPredicting(currentList2,Constant.HOST_LogLength);
//            predictnum2 = mathHandler.GM11PredictingTest(currentList2,Constant.HOST_LogLength);
            if(i+1 < originalDataram.size())
            {
                System.out.println("ori: "+ originalDataram.get(i+1) + "pre: " + predictnum2);
            }
            else
            {
                System.out.println("pre: " + predictnum2);
            }
            predictDataram.add(predictnum2);
        }

        BufferedWriter bw = new BufferedWriter(new FileWriter("D:\\java_workspace\\cloudsimplus-6.3.0-MyExample\\cloudsim-plus-examples\\src\\main\\java\\org\\cloudsimplus\\MyExample\\logs\\predictDatacpu.csv"));

        for(int i=0;i < predictData.size();i++) {
            Double num = predictData.get(i);
            bw.write(String.valueOf(num));
            bw.newLine();
            bw.flush();
        }

        bw = new BufferedWriter(new FileWriter("D:\\java_workspace\\cloudsimplus-6.3.0-MyExample\\cloudsim-plus-examples\\src\\main\\java\\org\\cloudsimplus\\MyExample\\logs\\predictDataram.csv"));
        for(int i=0;i < predictDataram.size();i++) {
            Double num = predictDataram.get(i);
            bw.write(String.valueOf(num));
            bw.newLine();
            bw.flush();
        }
        //释放资源
        bw.close();
        calRMSE(predictData,originalData);
        calMAE(predictData,originalData);
        calRMSE(predictDataram,originalDataram);
        calMAE(predictDataram,originalDataram);
        calMAPE(predictData,originalData);
        calMAPE(predictDataram,originalDataram);
    }

    public void calRMSE(ArrayList<Double> predictData,ArrayList<Double> originalData)
    {
        double totalpow = 0.0;
        for(int i = 0; i < predictData.size() - 1; ++i)
        {
            totalpow += Math.pow(predictData.get(i) - originalData.get(i+12), 2);
        }
        double RMSE = Math.sqrt(totalpow / predictData.size());
        System.out.println("RMSE : " + RMSE);
    }

    public void calMAE(ArrayList<Double> predictData,ArrayList<Double> originalData)
    {
        double totalmedian = 0.0;
        for(int i = 0; i < predictData.size() - 1; ++i)
        {
            totalmedian += Math.abs(predictData.get(i) - originalData.get(i+12));
        }
        double MAE = totalmedian / predictData.size();
        System.out.println("MAE : " + MAE);
    }

    public void calMAPE(ArrayList<Double> predictData,ArrayList<Double> originalData)
    {
        double totalmedian = 0.0;
        for(int i = 0; i < predictData.size() - 1; ++i)
        {
            totalmedian += Math.abs((predictData.get(i) - originalData.get(i+12)) / originalData.get(i+12));
        }
        double MAPE = totalmedian / predictData.size() * 100;
        System.out.println("MAPE : " + MAPE);
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

    public double ARIMRPredicting(List<Double> dataHistory,int n,double utilization,boolean max){
        if(!Constant.USING_PREDICT){
            return utilization;
        }
        //若历史记录不满足log长度，无法预测，直接返回当前利用率当作预测值
        if(dataHistory.size() < n || checkUtilizationNotAllTheSame(convertListToArray(dataHistory))){
            return utilization;
        }
        double predict = ARIMRPrediction(dataHistory,n);
        return predict;
    }

    public double LRPredicting(List<Double> dataHistory,int n,double utilization, Host host)
    {
        if(!Constant.USING_PREDICT){
            return utilization;
        }
        int len = 10;
        //若历史记录不满足log长度，无法预测，直接返回当前利用率当作预测值
        if(dataHistory.size() < len){
            return utilization;
        }
        double[] utilizationHistory = convertListToArray(dataHistory);
        double[] utilizationHistoryReversed = new double[len];
        for (int i = 0; i < len; i++) {
            utilizationHistoryReversed[i] = utilizationHistory[len - i - 1];
        }
        double[] estimates = null;
        try {
            estimates = getRobustLoessParameterEstimates(utilizationHistoryReversed);
        } catch (IllegalArgumentException e) {
            return utilization;
        }
        double migrationIntervals = Math.ceil(getMaximumVmMigrationTime(host) / Constant.SCHEDULING_INTERVAL);
        double predictedUtilization = estimates[0] + estimates[1] * (len + migrationIntervals) ;
        if(predictedUtilization < 0)
        {
            return utilization;
        }
        return predictedUtilization;
    }

    public double LRPredicting(List<Double> dataHistory,int n)
    {
        int len = 10;
        double[] utilizationHistory = convertListToArray(dataHistory);
        double[] utilizationHistoryReversed = new double[len];
        for (int i = 0; i < len; i++) {
            utilizationHistoryReversed[i] = utilizationHistory[len - i - 1];
        }
        double[] estimates = null;
        estimates = getLoessParameterEstimates(utilizationHistoryReversed);
        double predictedUtilization = estimates[0] + estimates[1] * (len);
        System.out.println(estimates.length + " " + estimates[0] + " " +estimates[1]);
        return predictedUtilization;
    }

    protected double getMaximumVmMigrationTime(Host host) {
        long maxRam = Long.MIN_VALUE;
        for (Vm vm : host.getVmList()) {
            long ram = vm.getRam().getCapacity();
            if (ram > maxRam) {
                maxRam = ram;
            }
        }
        return maxRam / ((double) host.getBw().getCapacity() / (2 * 8000));
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
        if(dataHistory.size() < n || checkUtilizationNotAllTheSame(convertListToArray(dataHistory))){
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
        if(dataHistory.size() < n || checkUtilizationNotAllTheSame(convertListToArray(dataHistory))){
            return utilization;
        }

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
            return findPredictMax(predicts);
        }else{
            return findPredictMin(predicts);
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
        return (originalSequence[0]-b/a) * Math.exp(-a * (k)) - (originalSequence[0]-b/a) * Math.exp(-a * (k-1));
    }

    public double getDGM21PredictResult(double a,double b,int k,double[] originalSequence){
        return getDGMcumulativePredictValue(a,b,k,originalSequence) - getDGMcumulativePredictValue(a,b,k-1,originalSequence);
    }

    public double getDGM11PredictResult(double a,double b,int k,double[] originalSequence){
        return getDGM11cumulativePredictValue(a,b,k,originalSequence) - getDGMcumulativePredictValue(a,b,k-1,originalSequence);
    }

    public double getDGMcumulativePredictValue(double a,double b,int k,double[] originalSequence){
//        return ( b/(a*a) - originalSequence[0]/a ) * Math.exp(-a * (k-1)) + (b/a) * (k) + originalSequence[0] * ((1+a)/a) - b/(a*a);
        return (b/(a*a) - originalSequence[0]/a) * Math.exp(-a * (k-1)) + b/a*(k-1) + (1+a)/a*originalSequence[0] - b/(a*a);
    }

    public double getDGM11cumulativePredictValue(double a,double b,int k,double[] originalSequence){
        return Math.pow(a,k-1) * originalSequence[0] + (1-Math.pow(a,k-1))/(1-a) * b;
    }

    //参与预测的利用率中不能有0，否则为奇异矩阵（不满秩），无法计算逆矩阵
    //利用率不能全部相同
    public boolean checkUtilizationNotAllTheSame(double[] originalSequence){
        double utilization = originalSequence[0];
        for(int i=1;i<originalSequence.length;++i){
            if(originalSequence[i] == utilization) return true;
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
        initialYN(YN,originalSequence,tn);
        double[][] result = calculateGM11AandB(B,YN);
        double a = result[0][0],b = result[1][0];
        double[] predicts = getKGM11PredictResult(a,b,n,originalSequence);
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

    public double distance(List<Double> usages){
        double[] data = convertListToArray(usages);
        Arrays.sort(data);
        double avg = usages.stream().mapToDouble(Double::doubleValue).sum()/usages.size();
        double totalD = 0.0;
        for(double num:data){
            totalD += Math.abs(num - avg);
        }
        return totalD / data.length;
    }

    public double distance2(List<Double> usages){
        double[] data = convertListToArray(usages);
        Arrays.sort(data);
        double max_value = usages.stream().mapToDouble(Double::doubleValue).max().getAsDouble();
        double median_value = getMedian(data);
        return max_value - median_value;
    }

    public double distance3(List<Double> usages){
        double[] data = convertListToArray(usages);
        Arrays.sort(data);
        double max_value = usages.stream().mapToDouble(Double::doubleValue).max().getAsDouble();
        double avg = getAvg(usages);
        return max_value - avg;
    }

    public double distance4(List<Double> usages){
        double[] data = convertListToArray(usages);
        Arrays.sort(data);
        double max_value = usages.stream().mapToDouble(Double::doubleValue).max().getAsDouble();
        double min_value = usages.stream().mapToDouble(Double::doubleValue).min().getAsDouble();
        return Math.abs(max_value - min_value);
    }

    public double distance5(List<Double> usages){
        double[] data = convertListToArray(usages);
        Arrays.sort(data);
        double max_value = usages.stream().mapToDouble(Double::doubleValue).max().getAsDouble();
        double min_value = usages.stream().mapToDouble(Double::doubleValue).min().getAsDouble();
        return Math.abs((max_value - min_value) / 2);
    }

    public double distance6(List<Double> usages) {
        double sd = getStandardDeviation(usages);
        return sd;
    }

    public double getMedian(double[] data){
        return getStatistics(data).getPercentile(50);
    }

    public double getAvg(List<Double> usages) {
        return usages.stream().mapToDouble(Double::doubleValue).sum()/usages.size();
    }

    public DescriptiveStatistics getStatistics(final double[] list) {
        DescriptiveStatistics stats = new DescriptiveStatistics();
        for (double v : list) {
            stats.addValue(v);
        }
        return stats;
    }

    public double getStandardDeviation(List<Double> usages) {
        double[] data = convertListToArray(usages);
        double avg = getAvg(usages);
        double totalpow = 0;
        for(double num : data){
            totalpow += Math.pow(num-avg,2);
        }
        return Math.sqrt(totalpow / data.length);
    }

    public double[] getLoessParameterEstimates(final double[] y) {
        int n = y.length;
        double[] x = new double[n];
        for (int i = 0; i < n; i++) {
            x[i] = i + 1;
        }
        return createWeigthedLinearRegression(x, y, getTricubeWeigts(n)).regress().getParameterEstimates();
    }

    public double[] getRobustLoessParameterEstimates(final double[] y) {
        int n = y.length;
        double[] x = new double[n];
        for (int i = 0; i < n; i++) {
            x[i] = i + 1;
        }
        SimpleRegression tricubeRegression = createWeigthedLinearRegression(x,
            y, getTricubeWeigts(n));
        double[] residuals = new double[n];
        for (int i = 0; i < n; i++) {
            residuals[i] = y[i] - tricubeRegression.predict(x[i]);
        }
        SimpleRegression tricubeBySquareRegression = createWeigthedLinearRegression(
            x, y, getTricubeBisquareWeigts(residuals));

        double[] estimates = tricubeBySquareRegression.regress()
            .getParameterEstimates();
        if (Double.isNaN(estimates[0]) || Double.isNaN(estimates[1])) {
            return tricubeRegression.regress().getParameterEstimates();
        }
        return estimates;
    }

    public double[] getTricubeBisquareWeigts(final double[] residuals) {
        int n = residuals.length;
        double[] weights = getTricubeWeigts(n);
        double[] weights2 = new double[n];
        double s6 = getMedian(abs(residuals)) * 6;
        for (int i = 2; i < n; i++) {
            double k = Math.pow(1 - Math.pow(residuals[i] / s6, 2), 2);
            if (k > 0) {
                weights2[i] = (1 / k) * weights[i];
            } else {
                weights2[i] = Double.MAX_VALUE;
            }
        }
        weights2[0] = weights2[1] = weights2[2];
        return weights2;
    }

    public static double[] abs(final double[] data) {
        double[] result = new double[data.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = Math.abs(data[i]);
        }
        return result;
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

    public List<Double> reverseList(List<Double> usages){
        List<Double> list = new LinkedList<>(usages);
        Collections.reverse(list);
        return list;
    }


}
