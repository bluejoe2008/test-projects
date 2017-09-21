import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import javax.imageio.ImageIO;

public class ImageTest
{
	static int NORMAL_WIDTH = 64;
	static int NORMAL_HEIGHT = 64;
	static String FILE_DIR = "/Users/bluejoe/testdata/pics";

	public static BufferedImage validateArea(File file) throws IOException
	{
		BufferedImage bi = ImageIO.read(file);
		// 获取当前图片的高,宽,ARGB
		int h = bi.getHeight();
		int w = bi.getWidth();
		int arr[][] = new int[w][h];

		// 获取图片每一像素点的灰度值
		for (int i = 0; i < w; i++)
		{
			for (int j = 0; j < h; j++)
			{
				// getRGB()返回默认的RGB颜色模型(十进制)
				arr[i][j] = getImageRgb(bi.getRGB(i, j));// 该点的灰度值
			}

		}

		int left = w - 1, top = h - 1, right = 0, bottom = 0;

		int FZ = 130;
		for (int i = 0; i < w; i++)
		{
			for (int j = 0; j < h; j++)
			{
				if (getGray(arr, i, j, w, h) > FZ)
				{
					if (i > right)
						right = i;
					if (j > bottom)
						bottom = j;
					if (i < left)
						left = i;
					if (j < top)
						top = j;
				}
			}
		}

		BufferedImage croped = bi.getSubimage(left, top, right - left + 1,
				bottom - top + 1);
		BufferedImage resized = new BufferedImage(NORMAL_WIDTH, NORMAL_HEIGHT,
				BufferedImage.TYPE_INT_ARGB);
		resized.getGraphics().drawImage(croped, 0, 0, NORMAL_WIDTH,
				NORMAL_HEIGHT, null);
		/*
		 * File file1 = new File(file.getPath() + ".1_.jpg");
		 * ImageIO.write(resized, "png", file1);
		 * 
		 * return ImageIO.read(file1);
		 */
		return resized;
	}

	public static void main(String[] args) throws IOException
	{
		File dir = new File(FILE_DIR);
		HashMap<String, ArrayList<Integer>> filePoints = new HashMap<String, ArrayList<Integer>>();
		for (File file : dir.listFiles())
		{
			if (!file.isFile() || file.isHidden()
					|| file.getPath().endsWith("_.jpg"))
				continue;

			try
			{
				BufferedImage bi = validateArea(file);
				// 获取当前图片的高,宽,ARGB
				int h = bi.getHeight();
				int w = bi.getWidth();
				int arr[][] = new int[w][h];

				// 获取图片每一像素点的灰度值
				for (int i = 0; i < w; i++)
				{
					for (int j = 0; j < h; j++)
					{
						// getRGB()返回默认的RGB颜色模型(十进制)
						arr[i][j] = getImageRgb(bi.getRGB(i, j));// 该点的灰度值
					}

				}

				BufferedImage bufferedImage = new BufferedImage(w, h,
						BufferedImage.TYPE_BYTE_BINARY);// 构造一个类型为预定义图像类型之一的
														// BufferedImage，TYPE_BYTE_BINARY（表示一个不透明的以字节打包的
														// 1、2 或 4 位图像。）

				// ArrayList<ArrayList<Integer>> arr2 = new
				// ArrayList<ArrayList<Integer>>();

				int FZ = 130;
				// System.err.println(file.getPath());
				ArrayList<Integer> points = new ArrayList<Integer>();
				for (int i = 0; i < h; i++)
				{
					for (int j = 0; j < w; j++)
					{
						if (getGray(arr, j, i, w, h) > FZ)
						{
							int black = new Color(255, 255, 255).getRGB();
							bufferedImage.setRGB(j, i, black);
							points.add(0);
						}
						else
						{
							int white = new Color(0, 0, 0).getRGB();
							bufferedImage.setRGB(j, i, white);
							points.add(1);
						}
					}
				}

				filePoints.put(file.getName(), points);
				System.err.println(String.format("(%s,%s)",
						file.getName().charAt(0) - '0', points));

				/*
				 * File file2 = new File(file.getPath() + ".2_.jpg");
				 * ImageIO.write(bufferedImage, "jpg", file2);
				 */
			}
			catch (Throwable e)
			{
				e.printStackTrace();
			}
		}
	}

	private static int getImageRgb(int i)
	{
		String argb = Integer.toHexString(i);// 将十进制的颜色值转为十六进制
		// argb分别代表透明,红,绿,蓝 分别占16进制2位
		int r = Integer.parseInt(argb.substring(2, 4), 16);// 后面参数为使用进制
		int g = Integer.parseInt(argb.substring(4, 6), 16);
		int b = Integer.parseInt(argb.substring(6, 8), 16);
		int result = (int) ((r + g + b) / 3);
		return result;
	}

	// 自己加周围8个灰度值再除以9，算出其相对灰度值
	public static int getGray(int gray[][], int x, int y, int w, int h)
	{
		int rs = gray[x][y] + (x == 0 ? 255 : gray[x - 1][y])
				+ (x == 0 || y == 0 ? 255 : gray[x - 1][y - 1])
				+ (x == 0 || y == h - 1 ? 255 : gray[x - 1][y + 1])
				+ (y == 0 ? 255 : gray[x][y - 1])
				+ (y == h - 1 ? 255 : gray[x][y + 1])
				+ (x == w - 1 ? 255 : gray[x + 1][y])
				+ (x == w - 1 || y == 0 ? 255 : gray[x + 1][y - 1])
				+ (x == w - 1 || y == h - 1 ? 255 : gray[x + 1][y + 1]);
		return rs / 9;
	}
}