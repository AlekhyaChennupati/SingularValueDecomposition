import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.io.File
import java.awt.image.BufferedImage
import javax.imageio.ImageIO
import breeze.linalg.svd.SVD
import breeze.linalg.qr.QR
import breeze.linalg.qrp.QRP
import breeze.linalg._
import breeze.linalg.svd.SVD
import scala.collection.mutable.ArrayBuffer

object ImageReading {

def main(args: Array[String]){
var startTime = java.lang.System.currentTimeMillis()
println("startTime="+startTime)
	val conf = new SparkConf().setAppName("SVD Application")
    	val sc = new SparkContext(conf)
    	var imgFiles = new ArrayBuffer[String]()

for(f<-0 to 1000) {
if(f<=9)
	imgFiles+= "/Users/Alya/Documents/spark-1.2.1-bin-hadoop2.4/sparkExperiments/SVDTest/images/000"+f+".png"    
else if(f>9 && f<=99)
	imgFiles+= "/Users/Alya/Documents/spark-1.2.1-bin-hadoop2.4/sparkExperiments/SVDTest/images/00"+f+".png"
else if(f>100 && f<=999)
	imgFiles+= "/Users/Alya/Documents/spark-1.2.1-bin-hadoop2.4/sparkExperiments/SVDTest/images/0"+f+".png"
}
var averageMatrix = breeze.linalg.DenseMatrix.zeros[Double](420,420)
var averageMatrixNoSVD = breeze.linalg.DenseMatrix.zeros[Double](420,420)
var forbeniousMatrix = breeze.linalg.DenseMatrix.zeros[Double](420,420)

var fparallel = sc.parallelize(imgFiles)

def computeSVDfunc_1(imgName:String, kValue:Int): DenseMatrix[Double] = {
val img = ImageIO.read(new File(imgName))
val rows = img.getHeight()
val cols = img.getWidth()
val imageMatrix = breeze.linalg.DenseMatrix.zeros[Double](420,420)
val imageMatrix2 = breeze.linalg.DenseMatrix.zeros[Double](420,420)

for ( i <- 0 until cols; j <- 0 until rows )
       imageMatrix(i,j) =(img.getRGB(j,i) & 0x000000ff).toDouble

val svd.SVD(u,s,vt) = svd(imageMatrix)
var stest = diag(s)
var reconstImage = breeze.linalg.DenseMatrix.zeros[Double](420,420)

reconstImage = u(0 until 420, Range(0,kValue))*stest(0 until kValue, Range(0,kValue))*vt(0 until kValue,Range(0,420))
reconstImage
}

def noSVDfunc(imgNameNoSVD:String):DenseMatrix[Double] = {
val imgNoSVD = ImageIO.read(new File(imgNameNoSVD))
val rows = imgNoSVD.getHeight()
val cols = imgNoSVD.getWidth()
val imageMatrixNoSVD = breeze.linalg.DenseMatrix.zeros[Double](420,420)

for ( i <- 0 until cols; j <- 0 until rows )
       imageMatrixNoSVD(i,j) =(imgNoSVD.getRGB(j,i) & 0x000000ff).toDouble
       
imageMatrixNoSVD
}

averageMatrix = fparallel.map(fname=>computeSVDfunc_1(fname,100)).reduce(_+_)
breeze.linalg.csvwrite(new File("/Users/Alya/Documents/spark-1.2.1-bin-hadoop2.4/sparkExperiments/SVDTest/matrixdata/avgmatrixk=100_changed.txt"),averageMatrix:/(imgFiles.length).toDouble,separator=' ')
averageMatrix = (averageMatrix:/(imgFiles.length).toDouble)

averageMatrixNoSVD = fparallel.map(fname=>noSVDfunc(fname)).reduce(_+_)
breeze.linalg.csvwrite(new File("/Users/Alya/Documents/spark-1.2.1-bin-hadoop2.4/sparkExperiments/SVDTest/matrixdata/avgmatrixNoSVD.txt"),averageMatrixNoSVD:/(imgFiles.length).toDouble,separator=' ')
averageMatrixNoSVD = (averageMatrixNoSVD:/(imgFiles.length).toDouble)

forbeniousMatrix = averageMatrix + (averageMatrixNoSVD:*(-1.0))

println("Frobenious Norm is "+norm(forbeniousMatrix.toDenseVector))

var endTime = java.lang.System.currentTimeMillis()
println("Total time = "+(endTime-startTime))
}
}
	
