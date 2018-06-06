package streaming.dsl.mmlib.algs.processing.image;

import org.apache.spark.sql.Row;

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
}
