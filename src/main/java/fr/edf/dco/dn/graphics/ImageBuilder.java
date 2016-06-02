package fr.edf.dco.dn.graphics;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.*;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by Anès Mahi on 13/05/2016.
 */
public class ImageBuilder {


    static int valuesFontSize ;
    static int titleFontSize;
    static String policeFont ;
    static int firstTextAdjustment;
    static int secondTextAdjustment;
    static int thirdTextAdjustment;
    static int imageWidth;
    static int imageHeight;
    static int bandWidth;
    static int firstBandStartingPoint;
    static int secondBandStartingPoint;
    static int thirdBandStartingPoint;
    static int baseFrameHeight;
    static int marginTop;
    static int textBandHeight;
    static int gap;

    public ImageBuilder(){}

    public ImageBuilder(Properties imageProperties){
        valuesFontSize = Integer.valueOf(imageProperties.getProperty("img.values.font.size"));
        titleFontSize = Integer.valueOf(imageProperties.getProperty("img.text.font.size"));
        policeFont = imageProperties.getProperty("img.police.font");

        firstTextAdjustment = Integer.valueOf(imageProperties.getProperty("img.first_value.text_adjustment"));
        secondTextAdjustment = Integer.valueOf(imageProperties.getProperty("img.second_value.text_adjustment"));
        thirdTextAdjustment = Integer.valueOf(imageProperties.getProperty("img.third_value.text_adjustment"));

        imageHeight = Integer.valueOf(imageProperties.getProperty("img.height"));
        imageWidth = Integer.valueOf(imageProperties.getProperty("img.width"));
        baseFrameHeight = Integer.valueOf(imageProperties.getProperty("img.base_frame.height"));

        bandWidth = Integer.valueOf(imageProperties.getProperty("img.band.width"));
        firstBandStartingPoint = Integer.valueOf(imageProperties.getProperty("img.first_band.starting_point"));
        secondBandStartingPoint = Integer.valueOf(imageProperties.getProperty("img.second_band.starting_point"));
        thirdBandStartingPoint = Integer.valueOf(imageProperties.getProperty("img.third_band.starting_point"));

        marginTop = Integer.valueOf(imageProperties.getProperty("img.margin_top"));
        textBandHeight = Integer.valueOf(imageProperties.getProperty("img.text_band.height"));
        gap = Integer.valueOf(imageProperties.getProperty("img.gap"));
    }

    static Color blanc_p = new Color(255, 255, 255, 255);
    static Color orange_p = new Color(255, 160, 49, 255);
    static Color bleu_p = new Color(1, 92, 186, 255);
    static Color vert_p = new Color(195, 214, 0, 255);
    static Color client_p = new Color(80, 80, 80, 255);

    /**
     * @param imgSocle
     * @param consoFoyersSimilaires
     * @param consoMonFoyer
     * @param consoFoyersEconomes
     * @return A new image generated from data in params
     */

    /*
    This first function is used for the first type of images (first mail)
     */
    static public BufferedImage buildImage(
            BufferedImage imgSocle,
            int consoFoyersSimilaires,
            int consoMonFoyer,
            int consoFoyersEconomes) {

        // calcul des dimensions utilisables de l'espace
        int espaceHeight = imageHeight - marginTop - baseFrameHeight - textBandHeight;
        int baseEspace = imageHeight - baseFrameHeight - textBandHeight;

        // Mise à l'échelle des données
        int maxConso = Math.max(Math.max(consoFoyersSimilaires, consoMonFoyer), consoFoyersEconomes);
        int minConso = 0; // Math.min(Math.min(consoFoyersSimilaires, consoMonFoyer),consoFoyersEconomes);
        int ecartConso = maxConso - minConso;
        double correctif = (double) (espaceHeight) / (double) (ecartConso);
        int consoFoyersSimilairesC = (int) Math.round(correctif * consoFoyersSimilaires);
        int consoMonFoyerC = (int) Math.round(correctif * consoMonFoyer);
        int consoFoyersEconomesC = (int) Math.round(correctif * consoFoyersEconomes);

        // Initialisation par une image blanche
        BufferedImage new_img = new BufferedImage(imageWidth, imageHeight, BufferedImage.TYPE_INT_RGB);
        Graphics g = new_img.getGraphics();
        g.setColor(blanc_p);
        g.fillRect(0, 0, imageWidth, imageHeight);

        // Ajout du socle
        g.drawImage(imgSocle, gap, (imageHeight - baseFrameHeight), null);

        g.setColor(orange_p);
        g.fillRect(firstBandStartingPoint, baseEspace - consoFoyersSimilairesC, bandWidth, consoFoyersSimilairesC + textBandHeight);
        g.setFont(new Font(policeFont, Font.PLAIN, titleFontSize));
        g.drawString("Foyers Similaires", firstBandStartingPoint - firstTextAdjustment, baseEspace - consoFoyersSimilairesC - 10);

        g.setColor(blanc_p);
        g.setFont(new Font(policeFont, Font.PLAIN, valuesFontSize));
        g.drawString(String.valueOf(consoFoyersSimilaires), firstBandStartingPoint + (bandWidth / 4), baseEspace + textBandHeight - 20);
        g.drawString("kWh", firstBandStartingPoint + (bandWidth / 4), baseEspace + textBandHeight);

        g.setColor(bleu_p);
        g.fillRect(secondBandStartingPoint, baseEspace - consoMonFoyerC, bandWidth, consoMonFoyerC + textBandHeight);
        g.setFont(new Font(policeFont, Font.PLAIN, titleFontSize));
        g.drawString("Mon Foyer", secondBandStartingPoint - secondTextAdjustment, baseEspace - consoMonFoyerC - 10);

        g.setColor(blanc_p);
        g.setFont(new Font(policeFont, Font.PLAIN, valuesFontSize));
        g.drawString(String.valueOf(consoMonFoyer), secondBandStartingPoint + (bandWidth / 4), baseEspace + textBandHeight - 20);
        g.drawString("kWh", secondBandStartingPoint + (bandWidth / 4), baseEspace + textBandHeight);

        g.setColor(vert_p);
        g.fillRect(thirdBandStartingPoint, baseEspace - consoFoyersEconomesC, bandWidth, consoFoyersEconomesC + textBandHeight);
        g.setFont(new Font(policeFont, Font.PLAIN, titleFontSize));
        g.drawString("Foyers similaires les", thirdBandStartingPoint - thirdTextAdjustment + 5, baseEspace - consoFoyersEconomesC - 30);
        g.drawString("moins consommateurs", thirdBandStartingPoint - thirdTextAdjustment, baseEspace - consoFoyersEconomesC - 10);

        g.setColor(blanc_p);
        g.setFont(new Font(policeFont, Font.PLAIN, valuesFontSize));
        g.drawString(String.valueOf(consoFoyersEconomes), thirdBandStartingPoint + (bandWidth / 4), baseEspace + textBandHeight - 20);
        g.drawString("kWh", thirdBandStartingPoint + (bandWidth / 4), baseEspace + textBandHeight);

        return (new_img);
    }
}
