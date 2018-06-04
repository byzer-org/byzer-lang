package streaming.dsl.mmlib.algs.processing.image;

import org.apache.spark.sql.Row;

import static org.bytedeco.javacpp.opencv_core.*;

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

    public static void release(IplImage cvImage) {
        if (cvImage != null) {
            cvImage.cvSize().close();
            cvReleaseImage(cvImage);
        }
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
