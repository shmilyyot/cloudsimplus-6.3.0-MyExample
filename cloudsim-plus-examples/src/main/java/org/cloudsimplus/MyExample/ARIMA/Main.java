package org.cloudsimplus.MyExample.ARIMA;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

public class Main
{
	public static void main(String args[])
	{
		File file = new File("cloudsim-plus-examples/src/main/java/org/cloudsimplus/MyExample/ARIMA/data.txt");
		try
		(
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)))
		)
		{
			String line = null;
			ArrayList<Double> al=new ArrayList<Double>();
			while ((line = br.readLine()) != null)
			{
				al.add(Double.parseDouble(line));
			}
			double [] data = new double[al.size()];
			for (int i = 0; i < data.length; ++i)
			{
				data[i] = al.get(i);
			}

			ARIMAModel arima = new ARIMAModel(data);

			ArrayList<double []> list = new ArrayList<>();
			int period = 1;
			int modelCnt = 5, cnt = 0;			//通过多次预测的平均值作为预测值
			double [] tmpPredict = new double [modelCnt];
			for (int k = 0; k < modelCnt; ++k)			//控制通过多少组参数进行计算最终的结果
			{
				double [] bestModel = arima.getARIMAModel(period, list, k != 0);
				if (bestModel.length == 0)
				{
					tmpPredict[k] = (int)data[data.length - period];
					cnt++;
					break;
				}
				else
				{
					double predictDiff = arima.predictValue((int)bestModel[0], (int)bestModel[1], period);
					tmpPredict[k] = arima.aftDeal(predictDiff, period);
					cnt++;
				}
				System.out.println("BestModel is " + bestModel[0] + " " + bestModel[1]);
				list.add(bestModel);
			}
			al.clear();
			double sumPredict = 0.0;
			for (int k = 0; k < cnt; ++k)
			{
				sumPredict += (double)tmpPredict[k] / (double)cnt;
			}
			double predict = sumPredict;
			System.out.println("Predict value="+predict);
		}
		catch (FileNotFoundException fnfe)
		{
			fnfe.printStackTrace();
		}
		catch (IOException ioe)
		{
			ioe.printStackTrace();
		}
	}
}
