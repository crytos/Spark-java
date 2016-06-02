package fr.edf.dco.dn.graphics;


import org.apache.spark.sql.Row;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.Properties;

/**
 * Created by Anès Mahi on 13/05/2016.
 * ImageFactory class uses ImageBuilder class to generate images using data corresponding to different mails type
 */

public class ImageFactory {

    /**
     * @param row The data that will be used to create the images (id_customer + the different values)
     */
    public BufferedImage createImage(Row row, Properties imageProperties) {


        BufferedImage imgBase;
        String baseGraphPath = imageProperties.getProperty("img.base_path");
        //System.out.println(baseGraphPath);
        try {
            imgBase = ImageIO.read(getClass().getResource(baseGraphPath));
            //ok //imgBase = ImageIO.read(ImageFactory.class.getResource(baseGraphPath));
            //local_Test ://imgBase = ImageIO.read(ImageFactory.class.getResource(baseGraphPath));
        } catch (IOException e) {
            System.out.println("Unable to load image from resources");
            imgBase = new BufferedImage(1, 1, BufferedImage.TYPE_INT_RGB);
        }
        ImageBuilder imgBuilder = new ImageBuilder(imageProperties);
        BufferedImage newImg = imgBuilder.buildImage(imgBase, (Integer) row.get(1), (Integer) row.get(2), (Integer) row.get(3));

        return newImg;
    }
}
