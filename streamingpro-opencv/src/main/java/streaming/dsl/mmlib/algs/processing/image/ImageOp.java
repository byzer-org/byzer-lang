package streaming.dsl.mmlib.algs.processing.image;

import org.apache.spark.sql.Row;
import org.imgscalr.Scalr;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;

import static org.bytedeco.javacpp.opencv_core.*;
import static org.bytedeco.javacpp.opencv_imgcodecs.*;
import static org.bytedeco.javacpp.opencv_imgproc.*;

/**
 * Created by allwefantasy on 28/5/2018.
 */
public class ImageOp {
    public static IplImage create(Row image) {
        IplImage iplImage = createHeader(ImageSchema.getWidth(image), ImageSchema.getHeight(image), ImageSchema.getDepth(image), ImageSchema.getNChannels(image));
        iplImage.imageData().put(ImageSchema.getData(image));
        return iplImage;
    }

    public static IplImage createHeader(int width, int height, int depth, int channel) {
        IplImage iplImage = cvCreateImage(
                cvSize(width, height), depth
                , channel

        );
        return iplImage;
    }

    public static void resize(IplImage cvImage, IplImage targetImage) {
        cvResize(cvImage, targetImage);
    }

    public static void release(IplImage cvImage) {
        if (cvImage != null) {
            cvImage.cvSize().close();
            cvReleaseImage(cvImage);
        }
    }

    public static void saveImage(String path, IplImage cvImage) {
        cvSaveImage(path, cvImage);
        release(cvImage);
    }

    public static byte[] readImage(String path) {
        IplImage image = cvLoadImage(path, CV_LOAD_IMAGE_UNCHANGED);
        byte[] sz = getData(image);
        release(image);
        return sz;
    }

    public static byte[] getData(IplImage image) {
        Mat m = new Mat(image);
        int sz = (int) (m.total() * m.channels());
        byte[] barr = new byte[sz];
        m.data().get(barr);
        m.close();
        return barr;
    }

    public static BufferedImage byte2image(byte[] b) {
        ByteArrayInputStream bArray = new ByteArrayInputStream(b);
        BufferedImage image = null;
        try {
            image = ImageIO.read(bArray);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bArray.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return image;
    }

    public static String imageFormat(byte[] b) {
        String format = null;
        try {
            ImageInputStream iis = ImageIO.createImageInputStream(new ByteArrayInputStream(b));
            Iterator<ImageReader> imageReadersList = ImageIO.getImageReaders(iis);
            if (!imageReadersList.hasNext()) {
                throw new RuntimeException("Image Readers Not Found!!!");
            }
            //Get the image type
            ImageReader reader = imageReadersList.next();
            format = reader.getFormatName();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return format;
    }


    public static byte[] image2byte(BufferedImage image, String format) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            ImageIO.write(image,format,out);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return out.toByteArray();
    }

    public static byte[] resize(byte[] b, Scalr.Method method, Scalr.Mode mode, int width, int height) {
        BufferedImage image = byte2image(b);
        BufferedImage targetImage = Scalr.resize(image, method, mode, width, height, Scalr.OP_ANTIALIAS);
        String format = imageFormat(b);
        byte[] data = image2byte(targetImage,format);
        return data;
    }
}
